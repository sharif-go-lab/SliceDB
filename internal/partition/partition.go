package partition

import (
	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/wal"
	"sync"
)

// Partition represents a data partition with key-value storage
type Partition struct {
	ID        int
	Role      model.NodeRole
	data      map[string]string
	keyLocks  map[string]*sync.Mutex
	mu        sync.RWMutex
	wal       *wal.WAL
	followers []string
}

// NewPartition creates a new data partition
func NewPartition(id int, role model.NodeRole) *Partition {
	return &Partition{
		ID:        id,
		Role:      role,
		data:      make(map[string]string),
		keyLocks:  make(map[string]*sync.Mutex),
		wal:       wal.NewWAL(),
		followers: make([]string, 0),
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
func (p *Partition) Set(key, value string) error {
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

		// Replicate to followers (in a real system, this would be asynchronous)
		p.replicateToFollowers(entry)

		return nil
	} else {
		// Follower nodes should only accept changes from leader
		// In this simplified model, we assume the call is from the leader
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
func (p *Partition) Delete(key string) error {
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

		// Replicate to followers
		p.replicateToFollowers(entry)

		return nil
	} else {
		// Follower nodes should only accept changes from leader
		p.mu.Lock()
		delete(p.data, key)
		p.mu.Unlock()
		return nil
	}
}

// ApplyLogEntry applies a log entry to the partition
func (p *Partition) ApplyLogEntry(entry model.LogEntry) {
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
}

// replicateToFollowers sends a log entry to all followers
// In a real implementation, this would use actual network calls
func (p *Partition) replicateToFollowers(entry model.LogEntry) {
	// This is a simplified version - in a real system, you'd use the network client
	// to send this entry to all followers asynchronously
}

// AddFollower adds a follower to the partition
func (p *Partition) AddFollower(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.followers = append(p.followers, nodeID)
}

// RemoveFollower removes a follower from the partition
func (p *Partition) RemoveFollower(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var updatedFollowers []string
	for _, id := range p.followers {
		if id != nodeID {
			updatedFollowers = append(updatedFollowers, id)
		}
	}
	p.followers = updatedFollowers
}

// ChangeRole changes the role of the partition (leader/follower)
func (p *Partition) ChangeRole(newRole model.NodeRole) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Role = newRole

	// If becoming a follower, clear data to receive fresh data from leader
	if newRole == model.NodeRoleFollower {
		p.data = make(map[string]string)
		p.wal.ClearAll()
	}
}
