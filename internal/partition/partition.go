package partition

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/storage"
	"github.com/sharif-go-lab/SliceDB/internal/wal"
)

var (
	ErrNotLeader   = errors.New("not the leader of this partition")
	ErrNotFollower = errors.New("partition is not a follower")
	ErrNotSynced   = errors.New("partition has not loaded a snapshot yet")
	ErrSealed      = errors.New("partition leadership is being handed off")
	ErrFrozen      = errors.New("key range is frozen while it moves to a new partition")
)

type Options struct {
	FlushSize int // memtable entries before it is frozen into an immutable level
	MaxLevels int // immutable levels before they are compacted
	WALRetain int // WAL entries kept for replicas that fall behind
}

type freeze struct {
	tag   string
	match func(string) bool
	until time.Time
}

// Partition is one replica of a partition on a node: an in-memory LSM store fed through a WAL.
//
// All mutations go through mu, so the order of entries in the WAL is exactly the order in which
// they were applied; followers replay the same order and end up with identical data. That makes
// separate per-key locks unnecessary: reads never take mu, and writes to one partition are cheap
// enough (two map operations) that serializing them is not the bottleneck.
type Partition struct {
	ID         int
	Generation int64

	mu          sync.Mutex
	role        model.NodeRole
	epoch       int64 // epoch of the current assignment (from the layout)
	dataEpoch   int64 // epoch of the leader our data came from
	prevEpoch   int64 // leader only: data epoch before the promotion
	promotedSeq int64 // leader only: sequence number at the promotion
	synced      bool
	sealed      bool
	keys        int
	freezes     []freeze

	store *storage.Store
	wal   *wal.WAL
}

func NewPartition(ref model.PartitionRef, opts Options) *Partition {
	return &Partition{
		ID:         ref.ID,
		Generation: ref.Generation,
		role:       model.NodeRoleFollower,
		prevEpoch:  -1,
		store:      storage.New(opts.FlushSize, opts.MaxLevels),
		wal:        wal.NewWAL(opts.WALRetain),
	}
}

func (p *Partition) Ref() model.PartitionRef {
	return model.PartitionRef{Generation: p.Generation, ID: p.ID}
}

// LeadFresh makes an empty partition the leader (a brand-new partition, or all copies were lost).
func (p *Partition) LeadFresh(epoch int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.resetLocked()
	p.role = model.NodeRoleLeader
	p.epoch = epoch
	p.dataEpoch = epoch
	p.prevEpoch = -1
	p.promotedSeq = 0
	p.synced = true
}

// Promote turns this replica into the leader of epoch, keeping its data. Followers that were
// replicating from the previous leader may keep their data if it is a prefix of ours.
func (p *Partition) Promote(epoch int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.synced {
		p.prevEpoch = p.dataEpoch
	} else {
		p.prevEpoch = -1
	}
	p.promotedSeq = p.wal.SequenceNumber()
	p.role = model.NodeRoleLeader
	p.epoch = epoch
	p.dataEpoch = epoch
	p.synced = true
	p.sealed = false
}

// Follow makes the partition a follower in epoch. A demoted leader throws its data away and
// downloads everything again from the new leader; it returns true in that case.
func (p *Partition) Follow(epoch int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	demoted := p.role == model.NodeRoleLeader
	if demoted {
		p.resetLocked()
	}
	p.role = model.NodeRoleFollower
	p.epoch = epoch
	p.sealed = false
	return demoted
}

// Reset drops all data so the next replication round starts from a snapshot.
func (p *Partition) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.resetLocked()
}

func (p *Partition) resetLocked() {
	p.store.Reset()
	p.wal.Reset(0)
	p.keys = 0
	p.synced = false
	p.dataEpoch = 0
	p.freezes = nil
}

// Seal stops (or resumes) accepting writes while leadership is handed off.
func (p *Partition) Seal(sealed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sealed = sealed
}

func (p *Partition) Set(key, value string) (model.LogEntry, error) {
	return p.write(model.Operation{Type: model.OpSet, Key: key, Value: value})
}

func (p *Partition) Delete(key string) (model.LogEntry, error) {
	return p.write(model.Operation{Type: model.OpDelete, Key: key})
}

func (p *Partition) Get(key string) (string, bool) {
	return p.store.Get(key)
}

func (p *Partition) write(op model.Operation) (model.LogEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.role != model.NodeRoleLeader {
		return model.LogEntry{}, ErrNotLeader
	}
	if p.sealed {
		return model.LogEntry{}, ErrSealed
	}
	if p.frozenLocked(op.Key) {
		return model.LogEntry{}, ErrFrozen
	}
	return p.appendLocked(op), nil
}

// Import writes ops through the WAL on the leader, ignoring seals and freezes. It is used to
// load data migrated from the previous generation while resharding.
func (p *Partition) Import(ops []model.Operation) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.role != model.NodeRoleLeader {
		return ErrNotLeader
	}
	for _, op := range ops {
		p.appendLocked(op)
	}
	return nil
}

// appendLocked logs op first (write-ahead), then applies it, then freezes the memtable into an
// immutable level with a WAL checkpoint once it is large enough.
func (p *Partition) appendLocked(op model.Operation) model.LogEntry {
	op.Timestamp = time.Now()
	entry := p.wal.Append(op)
	p.applyLocked(op, entry.SequenceNumber)

	if p.store.ShouldFlush() {
		checkpoint := p.wal.Checkpoint()
		p.store.Flush(checkpoint.SequenceNumber)
	}
	return entry
}

func (p *Partition) applyLocked(op model.Operation, seq int64) {
	switch op.Type {
	case model.OpSet:
		if _, exists := p.store.Get(op.Key); !exists {
			p.keys++
		}
		p.store.Put(op.Key, op.Value)
	case model.OpDelete:
		if _, exists := p.store.Get(op.Key); exists {
			p.keys--
			p.store.Delete(op.Key)
		}
	case model.OpCheckpoint:
		p.store.Flush(seq)
	}
}

// ApplyLogEntries replays entries streamed from the leader of leaderEpoch.
func (p *Partition) ApplyLogEntries(entries []model.LogEntry, leaderEpoch int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.role != model.NodeRoleFollower {
		return ErrNotFollower
	}
	if !p.synced {
		return ErrNotSynced
	}
	for _, entry := range entries {
		seq := p.wal.SequenceNumber()
		if entry.SequenceNumber <= seq {
			continue
		}
		if err := p.wal.Record(entry); err != nil {
			return err
		}
		p.applyLocked(entry.Operation, entry.SequenceNumber)
	}
	// The leader accepted our position, so our data is a prefix of its history.
	p.dataEpoch = leaderEpoch
	return nil
}

// Snapshot returns the partition's data (restricted to keys accepted by filter) as of a new WAL
// checkpoint. Only the memtable freeze happens under the lock; copying the immutable levels
// happens afterwards while writes continue into a fresh memtable.
func (p *Partition) Snapshot(filter func(string) bool) (model.Snapshot, error) {
	p.mu.Lock()
	if p.role != model.NodeRoleLeader {
		p.mu.Unlock()
		return model.Snapshot{}, ErrNotLeader
	}
	checkpoint := p.wal.Checkpoint()
	view := p.store.Flush(checkpoint.SequenceNumber)
	epoch := p.epoch
	p.mu.Unlock()

	return model.Snapshot{
		Generation:  p.Generation,
		PartitionID: p.ID,
		Epoch:       epoch,
		Seq:         checkpoint.SequenceNumber,
		Data:        view.Materialize(filter),
	}, nil
}

// LoadSnapshot replaces the follower's data with snap; replication continues after snap.Seq.
func (p *Partition) LoadSnapshot(snap model.Snapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.role != model.NodeRoleFollower {
		return ErrNotFollower
	}
	p.store.Load(snap.Data, snap.Seq)
	p.wal.Reset(snap.Seq)
	p.keys = len(snap.Data)
	p.dataEpoch = snap.Epoch
	p.synced = true
	return nil
}

// CanServe reports whether a follower whose data came from dataEpoch and ends at after can
// continue by fetching our log (otherwise it must reload a snapshot).
func (p *Partition) CanServe(dataEpoch, after int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if after > p.wal.SequenceNumber() {
		return false
	}
	if dataEpoch == p.epoch {
		return true
	}
	return p.prevEpoch >= 0 && dataEpoch == p.prevEpoch && after <= p.promotedSeq
}

// Export hands over the keys accepted by match to a partition of the next generation.
// Without freeze it returns a snapshot. With freeze it first stops writes to those keys (until
// ttl passes or the layout routes them elsewhere) and returns the matching WAL entries after
// `after`; if those were truncated it falls back to a snapshot taken after the freeze.
func (p *Partition) Export(tag string, match func(string) bool, freezeKeys bool, after int64, ttl time.Duration) (model.MigrationChunk, error) {
	if freezeKeys {
		p.mu.Lock()
		if p.role != model.NodeRoleLeader {
			p.mu.Unlock()
			return model.MigrationChunk{}, ErrNotLeader
		}
		p.addFreezeLocked(tag, match, ttl)
		entries, ok := p.wal.After(after, 0)
		epoch, seq := p.epoch, p.wal.SequenceNumber()
		p.mu.Unlock()

		if ok {
			var filtered []model.LogEntry
			for _, e := range entries {
				if e.Operation.Type != model.OpCheckpoint && match(e.Operation.Key) {
					filtered = append(filtered, e)
				}
			}
			return model.MigrationChunk{Mode: "logs", Epoch: epoch, Seq: seq, Entries: filtered}, nil
		}
	}

	snap, err := p.Snapshot(match)
	if err != nil {
		return model.MigrationChunk{}, err
	}
	return model.MigrationChunk{Mode: "snapshot", Epoch: snap.Epoch, Seq: snap.Seq, Data: snap.Data}, nil
}

func (p *Partition) addFreezeLocked(tag string, match func(string) bool, ttl time.Duration) {
	until := time.Now().Add(ttl)
	for i := range p.freezes {
		if p.freezes[i].tag == tag {
			p.freezes[i].until = until
			return
		}
	}
	p.freezes = append(p.freezes, freeze{tag: tag, match: match, until: until})
}

func (p *Partition) frozenLocked(key string) bool {
	if len(p.freezes) == 0 {
		return false
	}
	now := time.Now()
	active := p.freezes[:0]
	frozen := false
	for _, f := range p.freezes {
		if now.After(f.until) {
			continue
		}
		active = append(active, f)
		if f.match(key) {
			frozen = true
		}
	}
	p.freezes = active
	return frozen
}

func (p *Partition) Role() model.NodeRole {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.role
}

func (p *Partition) Epoch() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.epoch
}

func (p *Partition) DataEpoch() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.dataEpoch
}

func (p *Partition) Synced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.synced
}

func (p *Partition) Keys() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.keys
}

func (p *Partition) SequenceNumber() int64 {
	return p.wal.SequenceNumber()
}

func (p *Partition) GetLogsAfter(seq int64, max int) ([]model.LogEntry, bool) {
	return p.wal.After(seq, max)
}

// Changed returns a channel closed on the next WAL append; used to long-poll replication.
func (p *Partition) Changed() <-chan struct{} {
	return p.wal.Changed()
}

func (p *Partition) Status() model.PartitionStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	memtable, levels := p.store.Stats()
	return model.PartitionStatus{
		Generation: p.Generation,
		ID:         p.ID,
		Role:       p.role,
		Epoch:      p.epoch,
		DataEpoch:  p.dataEpoch,
		Seq:        p.wal.SequenceNumber(),
		Synced:     p.synced,
		Sealed:     p.sealed,
		Keys:       p.keys,
		MemTable:   memtable,
		Levels:     levels,
		WAL:        p.wal.Len(),
	}
}

func (p *Partition) String() string {
	return fmt.Sprintf("%s(%s, epoch %d)", p.Ref(), p.Role(), p.Epoch())
}
