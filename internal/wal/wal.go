package wal

import (
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

type WAL struct {
	logs       []model.LogEntry
	currentSeq int64
	mu         sync.RWMutex
}

func NewWAL() *WAL {
	return &WAL{
		logs:       make([]model.LogEntry, 0),
		currentSeq: 0,
	}
}

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

func (w *WAL) ClearAll() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.logs = make([]model.LogEntry, 0)
	w.currentSeq = 0
}

func (w *WAL) SequenceNumber() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.currentSeq
}
