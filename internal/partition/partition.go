package partition

import (
	"fmt"
	"sync"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/wal"
)

// Partition represents a data partition with key-value storage
type Partition struct {
	ID         int
	Role       model.NodeRole
	data       map[string]string
	keyLocks   map[string]*sync.Mutex
	mu         sync.RWMutex
	wal        *wal.WAL
	followers  []string
	lastUpdate int64
}

// NewPartition creates a new data partition
func NewPartition(id int, role model.NodeRole) *Partition {
	return &Partition{
		ID:         id,
		Role:       role,
		data:       make(map[string]string),
		keyLocks:   make(map[string]*sync.Mutex),
		wal:        wal.NewWAL(),
		followers:  make([]string, 0),
		lastUpdate: 0,
	}
}

// acquireKeyLock gets or creates a mutex for a specific key
func (p *Partition) acquireKeyLock(key string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.keyLocks[key]; !exists {
		p.keyLocks[key] = &sync.Mutex{}
	}
	return p.keyLocks[key]
}

// Set adds or updates a key-value pair
func (p *Partition) Set(key, value string) *model.LogEntry {
	// Lock the specific key for concurrent operations
	keyLock := p.acquireKeyLock(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	// If this is a leader node, update WAL and replicate
	if p.Role == model.NodeRoleLeader {
		// Add to WAL
		entry := p.wal.AppendSet(key, value)

		// Update data
		p.mu.Lock()
		p.data[key] = value
		p.mu.Unlock()

		return &entry
	} else {
		p.mu.Lock()
		p.data[key] = value
		p.mu.Unlock()

		return nil
	}
}

// Get retrieves a value by key
func (p *Partition) Get(key string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, exists := p.data[key]
	return value, exists
}

// Delete removes a key-value pair
func (p *Partition) Delete(key string) *model.LogEntry {
	// Lock the specific key
	keyLock := p.acquireKeyLock(key)
	keyLock.Lock()
	defer keyLock.Unlock()

	// If leader, update WAL and replicate
	if p.Role == model.NodeRoleLeader {
		// Add to WAL
		entry := p.wal.AppendDelete(key)

		// Delete from data
		p.mu.Lock()
		delete(p.data, key)
		p.mu.Unlock()

		return &entry
	} else {
		p.mu.Lock()
		delete(p.data, key)
		p.mu.Unlock()

		return nil
	}
}

// ApplyLogEntry applies a log entry to the partition
func (p *Partition) ApplyLogEntry(entry model.LogEntry) error {
	if p.wal.SequenceNumber()+1 != entry.SequenceNumber {
		return fmt.Errorf("partition data is not sync")
	}

	switch entry.Operation.Type {
	case "set":
		p.mu.Lock()
		p.data[entry.Operation.Key] = entry.Operation.Value
		p.mu.Unlock()
	case "delete":
		p.mu.Lock()
		delete(p.data, entry.Operation.Key)
		p.mu.Unlock()
	}
	return nil
}

func (p *Partition) UpdateFollowers(nodeIDs []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.followers = nodeIDs
}

// ChangeRole changes the role of the partition (leader/follower)
func (p *Partition) ChangeRole(newRole model.NodeRole) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Role = newRole

	// If becoming a follower, clear data to receive fresh data from leader
	if newRole == model.NodeRoleFollower {
		p.lastUpdate = 0
		p.data = make(map[string]string)
		p.wal.ClearAll()
	}
}

func (p *Partition) Followers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.followers
}

func (p *Partition) Items() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.data
}

func (p *Partition) SyncItems(items map[string]string, lastSync int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data = make(map[string]string, len(items))
	for k, v := range items {
		p.data[k] = v
	}
	p.lastUpdate = lastSync
	p.wal.ClearAll()
}
