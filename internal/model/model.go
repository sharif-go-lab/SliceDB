package model

import (
	"fmt"
	"time"
)

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

func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func RetryJob(job func() error) (err error) {
	ticker := time.Tick(500 * time.Millisecond)
	timeout := time.After(15 * time.Second)

	for {
		select {
		case <-timeout:
			if err == nil {
				err = fmt.Errorf("timeout exceeded")
			}
			return err
		case <-ticker:
			if err = job(); err == nil {
				return nil
			}
		}
	}
}
