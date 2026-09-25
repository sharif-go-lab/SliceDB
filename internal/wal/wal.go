package wal

import (
	"fmt"
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

// WAL is a partition's in-memory write-ahead log. Entries carry contiguous sequence numbers.
// Only the newest `retain` entries are kept; a replica that needs older ones has to load a
// snapshot instead (see partition.Snapshot).
type WAL struct {
	mu             sync.Mutex
	logs           []model.LogEntry // logs[i].SequenceNumber == base+1+i
	base           int64
	currentSeq     int64
	lastCheckpoint int64
	retain         int
	changed        chan struct{}
}

func NewWAL(retain int) *WAL {
	if retain <= 0 {
		retain = 100000
	}
	return &WAL{
		retain:  retain,
		changed: make(chan struct{}),
	}
}

// Append assigns the next sequence number to op and records it.
func (w *WAL) Append(op model.Operation) model.LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	if op.Timestamp.IsZero() {
		op.Timestamp = time.Now()
	}
	entry := model.LogEntry{SequenceNumber: w.currentSeq + 1, Operation: op}
	w.appendLocked(entry)
	return entry
}

func (w *WAL) AppendSet(key, value string) model.LogEntry {
	return w.Append(model.Operation{Type: model.OpSet, Key: key, Value: value})
}

func (w *WAL) AppendDelete(key string) model.LogEntry {
	return w.Append(model.Operation{Type: model.OpDelete, Key: key})
}

// Checkpoint records that the memtable was frozen into an immutable level at this point.
func (w *WAL) Checkpoint() model.LogEntry {
	return w.Append(model.Operation{Type: model.OpCheckpoint})
}

// Record appends an entry received from the leader. It must directly follow the current sequence.
func (w *WAL) Record(entry model.LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry.SequenceNumber != w.currentSeq+1 {
		return fmt.Errorf("SequenceNumber expected to be %d, %d found", w.currentSeq+1, entry.SequenceNumber)
	}
	w.appendLocked(entry)
	return nil
}

func (w *WAL) appendLocked(entry model.LogEntry) {
	w.logs = append(w.logs, entry)
	w.currentSeq = entry.SequenceNumber
	if entry.Operation.Type == model.OpCheckpoint {
		w.lastCheckpoint = entry.SequenceNumber
	}

	if len(w.logs) > w.retain+w.retain/2 {
		drop := len(w.logs) - w.retain
		w.base = w.logs[drop-1].SequenceNumber
		w.logs = append([]model.LogEntry(nil), w.logs[drop:]...)
	}

	close(w.changed)
	w.changed = make(chan struct{})
}

// After returns up to max entries (0 = all) that follow seq. ok is false when some of those
// entries were already truncated, i.e. the caller has to start over from a snapshot.
func (w *WAL) After(seq int64, max int) (entries []model.LogEntry, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if seq < w.base {
		return nil, false
	}
	if seq >= w.currentSeq {
		return nil, true
	}
	start := int(seq - w.base)
	end := len(w.logs)
	if max > 0 && end-start > max {
		end = start + max
	}
	entries = make([]model.LogEntry, end-start)
	copy(entries, w.logs[start:end])
	return entries, true
}

// GetLogsAfter returns every retained entry after seq.
func (w *WAL) GetLogsAfter(seq int64) []model.LogEntry {
	entries, _ := w.After(seq, 0)
	return entries
}

// Reset empties the log and continues numbering after seq (used after loading a snapshot).
func (w *WAL) Reset(seq int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logs = nil
	w.base = seq
	w.currentSeq = seq
	w.lastCheckpoint = seq
	close(w.changed)
	w.changed = make(chan struct{})
}

func (w *WAL) ClearAll() {
	w.Reset(0)
}

func (w *WAL) SequenceNumber() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.currentSeq
}

func (w *WAL) LastCheckpoint() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.lastCheckpoint
}

func (w *WAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.logs)
}

// Changed returns a channel that is closed on the next append (or reset).
func (w *WAL) Changed() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.changed
}
