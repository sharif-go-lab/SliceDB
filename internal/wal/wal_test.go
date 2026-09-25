package wal

import (
	"testing"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

func TestAfterAndTruncation(t *testing.T) {
	w := NewWAL(10)
	for i := 0; i < 40; i++ {
		w.AppendSet("k", "v")
	}
	if w.SequenceNumber() != 40 {
		t.Fatalf("seq = %d, want 40", w.SequenceNumber())
	}
	if _, ok := w.After(0, 0); ok {
		t.Fatal("entries after 0 should have been truncated")
	}
	entries, ok := w.After(35, 0)
	if !ok || len(entries) != 5 || entries[0].SequenceNumber != 36 {
		t.Fatalf("After(35) = %d entries (ok=%v)", len(entries), ok)
	}
	if entries, ok := w.After(40, 0); !ok || len(entries) != 0 {
		t.Fatal("nothing should follow the last entry")
	}
	if entries, _ := w.After(30, 3); len(entries) != 3 {
		t.Fatalf("max not honored: %d entries", len(entries))
	}
}

func TestRecordRequiresContiguousSequence(t *testing.T) {
	w := NewWAL(0)
	w.Reset(10)
	if err := w.Record(model.LogEntry{SequenceNumber: 12}); err == nil {
		t.Fatal("a gap must be rejected")
	}
	if err := w.Record(model.LogEntry{SequenceNumber: 11, Operation: model.Operation{Type: model.OpCheckpoint}}); err != nil {
		t.Fatal(err)
	}
	if w.LastCheckpoint() != 11 {
		t.Fatalf("checkpoint = %d, want 11", w.LastCheckpoint())
	}
}

func TestChangedIsClosedOnAppend(t *testing.T) {
	w := NewWAL(0)
	ch := w.Changed()
	w.AppendDelete("k")
	select {
	case <-ch:
	default:
		t.Fatal("Changed channel was not closed by an append")
	}
}
