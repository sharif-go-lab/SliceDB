package partition

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

var ref = model.PartitionRef{Generation: 1, ID: 0}

func newLeader(opts Options) *Partition {
	p := NewPartition(ref, opts)
	p.LeadFresh(1)
	return p
}

func catchUp(t *testing.T, leader, follower *Partition) {
	t.Helper()
	if !follower.Synced() {
		snap, err := leader.Snapshot(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := follower.LoadSnapshot(snap); err != nil {
			t.Fatal(err)
		}
	}
	if !leader.CanServe(follower.DataEpoch(), follower.SequenceNumber()) {
		t.Fatal("leader refuses to serve the follower")
	}
	entries, ok := leader.GetLogsAfter(follower.SequenceNumber(), 0)
	if !ok {
		t.Fatal("log truncated")
	}
	if err := follower.ApplyLogEntries(entries, leader.Epoch()); err != nil {
		t.Fatal(err)
	}
}

func assertSame(t *testing.T, a, b *Partition, keys int) {
	t.Helper()
	for i := 0; i < keys; i++ {
		k := fmt.Sprint("k", i)
		va, oka := a.Get(k)
		vb, okb := b.Get(k)
		if va != vb || oka != okb {
			t.Fatalf("%s: %q/%v vs %q/%v", k, va, oka, vb, okb)
		}
	}
	if a.Keys() != b.Keys() {
		t.Fatalf("key counts differ: %d vs %d", a.Keys(), b.Keys())
	}
}

func TestReplicationViaSnapshotAndLog(t *testing.T) {
	leader := newLeader(Options{FlushSize: 16, MaxLevels: 2})
	follower := NewPartition(ref, Options{})
	follower.Follow(1)

	for i := 0; i < 100; i++ {
		leader.Set(fmt.Sprint("k", i), fmt.Sprint(i))
	}
	catchUp(t, leader, follower) // snapshot + tail

	for i := 0; i < 100; i += 3 {
		leader.Delete(fmt.Sprint("k", i))
	}
	for i := 50; i < 150; i++ {
		leader.Set(fmt.Sprint("k", i), "new")
	}
	catchUp(t, leader, follower) // log only
	assertSame(t, leader, follower, 150)

	if _, err := follower.Set("x", "y"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower accepted a write: %v", err)
	}
}

func TestFollowerContinuesAcrossFailover(t *testing.T) {
	old := newLeader(Options{})
	a := NewPartition(ref, Options{})
	b := NewPartition(ref, Options{})
	a.Follow(1)
	b.Follow(1)
	for i := 0; i < 10; i++ {
		old.Set(fmt.Sprint("k", i), "v")
	}
	catchUp(t, old, a)
	catchUp(t, old, b)
	for i := 10; i < 20; i++ {
		old.Set(fmt.Sprint("k", i), "v")
	}
	catchUp(t, old, a) // a is ahead of b now

	// old dies; the more up-to-date replica a becomes leader of epoch 2.
	a.Promote(2)
	b.Follow(2)
	if !a.CanServe(b.DataEpoch(), b.SequenceNumber()) {
		t.Fatal("b's data is a prefix of a's history and should continue without a snapshot")
	}
	catchUp(t, a, b)
	assertSame(t, a, b, 20)

	// A replica that is ahead of the new leader has diverged and must resync.
	if a.CanServe(1, a.SequenceNumber()+5) {
		t.Fatal("a replica ahead of the leader must not continue")
	}
}

func TestDemotedLeaderDiscardsData(t *testing.T) {
	p := newLeader(Options{})
	p.Set("k", "v")
	if !p.Follow(2) {
		t.Fatal("Follow should report the demotion")
	}
	if _, ok := p.Get("k"); ok || p.Synced() {
		t.Fatal("a demoted leader must throw its data away and resync")
	}
}

func TestSnapshotDoesNotBlockWrites(t *testing.T) {
	leader := newLeader(Options{FlushSize: 1 << 20})
	for i := 0; i < 1000; i++ {
		leader.Set(fmt.Sprint("k", i), "v1")
	}
	snap, _ := leader.Snapshot(nil)
	for i := 0; i < 1000; i++ {
		leader.Set(fmt.Sprint("k", i), "v2")
	}
	for k, v := range snap.Data {
		if v != "v1" {
			t.Fatalf("snapshot saw a later write: %s=%s", k, v)
		}
	}
	if len(snap.Data) != 1000 {
		t.Fatalf("snapshot has %d keys", len(snap.Data))
	}
}

func TestSealAndFreeze(t *testing.T) {
	p := newLeader(Options{})
	p.Seal(true)
	if _, err := p.Set("a", "1"); !errors.Is(err, ErrSealed) {
		t.Fatalf("sealed partition accepted a write: %v", err)
	}
	p.Seal(false)

	p.Set("move-1", "1")
	p.Set("stay-1", "1")
	snap, _ := p.Export("t", func(k string) bool { return k[:4] == "move" }, false, 0, time.Minute)
	if len(snap.Data) != 1 {
		t.Fatalf("export snapshot = %v", snap.Data)
	}
	p.Set("move-2", "2")
	chunk, err := p.Export("t", func(k string) bool { return k[:4] == "move" }, true, snap.Seq, time.Minute)
	if err != nil || chunk.Mode != "logs" || len(chunk.Entries) != 1 || chunk.Entries[0].Operation.Key != "move-2" {
		t.Fatalf("frozen export = %+v, %v", chunk, err)
	}
	if _, err := p.Set("move-3", "3"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("frozen key accepted a write: %v", err)
	}
	if _, err := p.Set("stay-2", "2"); err != nil {
		t.Fatalf("unrelated key rejected: %v", err)
	}
}

func TestTruncatedLogFallsBackToSnapshot(t *testing.T) {
	leader := newLeader(Options{WALRetain: 10})
	follower := NewPartition(ref, Options{})
	follower.Follow(1)
	catchUp(t, leader, follower)
	for i := 0; i < 100; i++ {
		leader.Set(fmt.Sprint("k", i), "v")
	}
	if _, ok := leader.GetLogsAfter(follower.SequenceNumber(), 0); ok {
		t.Fatal("expected the log to be truncated")
	}
	follower.Reset()
	catchUp(t, leader, follower)
	assertSame(t, leader, follower, 100)
}
