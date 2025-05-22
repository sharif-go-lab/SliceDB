package wal

import (
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

// WAL represents a Write-Ahead Log
type WAL struct {
	logs       []model.LogEntry
	currentSeq int64
	mu         sync.RWMutex
}

// NewWAL creates a new in-memory WAL
func NewWAL() *WAL {
	return &WAL{
		logs:       make([]model.LogEntry, 0),
		currentSeq: 0,
	}
}

// AppendSet adds a SET operation to the WAL
func (w *WAL) AppendSet(key, value string) model.LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.currentSeq++
	entry := model.LogEntry{
		SequenceNumber: w.currentSeq,
		Operation: model.Operation{
			Type:      "set",
			Key:       key,
			Value:     value,
			Timestamp: time.Now(),
		},
	}

	w.logs = append(w.logs, entry)
	return entry
}

// AppendDelete adds a DELETE operation to the WAL
func (w *WAL) AppendDelete(key string) model.LogEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.currentSeq++
	entry := model.LogEntry{
		SequenceNumber: w.currentSeq,
		Operation: model.Operation{
			Type:      "delete",
			Key:       key,
			Timestamp: time.Now(),
		},
	}

	w.logs = append(w.logs, entry)
	return entry
}

// GetLogsAfter returns all logs with sequence number greater than the given sequence
func (w *WAL) GetLogsAfter(seq int64) []model.LogEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var result []model.LogEntry
	for _, log := range w.logs {
		if log.SequenceNumber > seq {
			result = append(result, log)
		}
	}
	return result
}

// ClearAll removes all logs (used when node role changes)
func (w *WAL) ClearAll() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logs = make([]model.LogEntry, 0)
	w.currentSeq = 0
}
