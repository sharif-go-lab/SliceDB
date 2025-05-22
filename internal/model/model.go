package model

import "time"

// NodeStatus represents the status of a node
type NodeStatus string

const (
	NodeStatusHealthy   NodeStatus = "healthy"
	NodeStatusUnhealthy NodeStatus = "unhealthy"
)

// NodeRole represents the role of a node
type NodeRole string

const (
	NodeRoleLeader   NodeRole = "leader"
	NodeRoleFollower NodeRole = "follower"
)

// Node represents a database node in the cluster
type Node struct {
	ID       string
	Address  string
	Status   NodeStatus
	LastSeen time.Time
}

// Partition represents a data partition
type Partition struct {
	ID          int
	LeaderID    string
	FollowerIDs []string
}

// Operation represents a database operation (for WAL)
type Operation struct {
	Type      string // "set" or "delete"
	Key       string
	Value     string
	Timestamp time.Time
}

// LogEntry represents an entry in the Write-Ahead Log
type LogEntry struct {
	SequenceNumber int64
	Operation      Operation
}
