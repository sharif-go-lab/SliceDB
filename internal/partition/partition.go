package partition

import (
	"fmt"
	"sync"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/wal"
)

type Partition struct {
	ID       int
	Role     model.NodeRole
	data     map[string]string
	keyLocks map[string]*sync.Mutex
	mu       sync.RWMutex
	wal      *wal.WAL
}

func NewPartition(id int, role model.NodeRole) *Partition {
	return &Partition{
		ID:       id,
		Role:     role,
		data:     make(map[string]string),
		keyLocks: make(map[string]*sync.Mutex),
		wal:      wal.NewWAL(),
	}
}

func (p *Partition) acquireKeyLock(key string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.keyLocks[key]; !exists {
		p.keyLocks[key] = &sync.Mutex{}
	}
	return p.keyLocks[key]
}

func (p *Partition) Set(key, value string) *model.LogEntry {
	// Lock the specific key for concurrent operations
	keyLock := p.acquireKeyLock(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Update data
	if p.data[key] == value {
		return nil
	}
	p.data[key] = value

	// If this is a leader node, update WAL and replicate
	if p.Role == model.NodeRoleLeader {
		entry := p.wal.AppendSet(key, value)
		return &entry
	}
	return nil
}

func (p *Partition) Get(key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, exists := p.data[key]
	return value, exists
}

func (p *Partition) Delete(key string) *model.LogEntry {
	// Lock the specific key
	keyLock := p.acquireKeyLock(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Delete from data
	if _, exists := p.data[key]; !exists {
		return nil
	}
	delete(p.data, key)

	// If this is a leader node, update WAL and replicate
	if p.Role == model.NodeRoleLeader {
		entry := p.wal.AppendDelete(key)
		return &entry
	}
	return nil
}

func (p *Partition) ApplyLogEntries(entries []model.LogEntry) error {
	for _, entry := range entries {
		if p.wal.SequenceNumber() >= entry.SequenceNumber {
			continue
		}
		if p.wal.SequenceNumber()+1 < entry.SequenceNumber {
			return fmt.Errorf("SequenceNumber expected to be %d, %d found", p.wal.SequenceNumber()+1, entry.SequenceNumber)
		}

		p.mu.Lock()
		switch entry.Operation.Type {
		case "set":
			p.data[entry.Operation.Key] = entry.Operation.Value
			p.wal.AppendSet(entry.Operation.Key, entry.Operation.Value)
		case "delete":
			delete(p.data, entry.Operation.Key)
			p.wal.AppendDelete(entry.Operation.Key)
		}
		p.mu.Unlock()
	}
	return nil
}

func (p *Partition) ChangeRole(newRole model.NodeRole) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Role = newRole
}

func (p *Partition) SequenceNumber() int64 {
	return p.wal.SequenceNumber()
}

func (p *Partition) GetLogsAfter(seq int64) []model.LogEntry {
	return p.wal.GetLogsAfter(seq)
}
