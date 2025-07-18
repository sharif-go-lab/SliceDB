package model

import (
	"fmt"
	"time"
)

type NodeRole string

const (
	NodeRoleLeader   NodeRole = "leader"
	NodeRoleFollower NodeRole = "follower"
)

type Node struct {
	ID      string
	Address string
}

type Partition struct {
	ID          int
	LeaderID    string
	FollowerIDs []string
}

type Operation struct {
	Type      string // "set" or "delete"
	Key       string
	Value     string
	Timestamp time.Time
}

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
