package model

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sharif-go-lab/SliceDB/pkg/hash"
)

type NodeRole string

const (
	NodeRoleLeader   NodeRole = "leader"
	NodeRoleFollower NodeRole = "follower"
)

// Node is the membership record a database node keeps alive in etcd under a lease.
// Incarnation changes on every process start (and after a lost lease), which lets the
// controller tell a restarted node — whose memory is empty — from one that never went away.
type Node struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	Incarnation int64  `json:"incarnation"`
}

// Config is the desired shape of the cluster. It lives in etcd and is edited from the panel.
type Config struct {
	PartitionCount    int `json:"partition_count"`
	ReplicationFactor int `json:"replication_factor"`
}

type PartitionState string

const (
	PartitionActive PartitionState = "active"
	// PartitionHandoff means leadership is moving to HandoffTo; writes are paused meanwhile.
	PartitionHandoff PartitionState = "handoff"
)

type Partition struct {
	ID    int   `json:"id"`
	Epoch int64 `json:"epoch"`
	// Leader takes every write. Followers are in-sync replicas that may serve reads.
	// Joining replicas are still loading a snapshot / catching up on the WAL.
	Leader    string   `json:"leader"`
	Followers []string `json:"followers"`
	Joining   []string `json:"joining"`
	// Evict lists holders that should be removed once enough other copies are in sync.
	Evict          []string       `json:"evict,omitempty"`
	State          PartitionState `json:"state"`
	HandoffTo      string         `json:"handoff_to,omitempty"`
	HandoffAt      int64          `json:"handoff_at,omitempty"`
	HandoffVersion int64          `json:"handoff_version,omitempty"`
	// Pinned partitions were placed by hand from the panel; the auto-balancer leaves them alone.
	Pinned bool `json:"pinned,omitempty"`
}

// Holders returns every node that keeps a copy of the partition.
func (p *Partition) Holders() []string {
	var holders []string
	if p.Leader != "" {
		holders = append(holders, p.Leader)
	}
	holders = append(holders, p.Followers...)
	return append(holders, p.Joining...)
}

func (p *Partition) Has(nodeID string) bool {
	return p.Leader == nodeID || ContainsString(p.Followers, nodeID) || ContainsString(p.Joining, nodeID)
}

// Writable reports whether writes can currently be sent to the leader.
func (p *Partition) Writable() bool {
	return p.Leader != "" && p.State != PartitionHandoff
}

// ReadReplicas returns the nodes allowed to serve reads: the leader first, then in-sync followers.
func (p *Partition) ReadReplicas() []string {
	var replicas []string
	if p.Leader != "" {
		replicas = append(replicas, p.Leader)
	}
	return append(replicas, p.Followers...)
}

type MigrationStatus string

const (
	MigrationPending MigrationStatus = "pending"
	MigrationDone    MigrationStatus = "done"
)

// Resharding is the intermediate state while the partition count changes: the next generation's
// partitions exist next to the current ones and keys move over one target partition at a time.
type Resharding struct {
	Generation     int64             `json:"generation"`
	PartitionCount int               `json:"partition_count"`
	Partitions     []Partition       `json:"partitions"`
	Status         []MigrationStatus `json:"status"`
}

// PartitionRef names a partition of a given generation (partition 3 of generation 1 and
// partition 3 of generation 2 hold different key sets).
type PartitionRef struct {
	Generation int64 `json:"generation"`
	ID         int   `json:"id"`
}

func (r PartitionRef) String() string {
	return fmt.Sprintf("g%d/p%d", r.Generation, r.ID)
}

// Layout is the routing table: which node leads / replicates which partition. It is stored
// under a single etcd key, written only by the elected controller and watched by everyone else.
type Layout struct {
	Version        int64       `json:"version"`
	Generation     int64       `json:"generation"`
	PartitionCount int         `json:"partition_count"`
	Partitions     []Partition `json:"partitions"`
	Resharding     *Resharding `json:"resharding,omitempty"`
	// Members maps node id to the incarnation the assignments above were made for.
	Members map[string]int64 `json:"members"`
}

// Route returns the partition that currently owns key. While resharding, a key is served by its
// new partition once that partition's migration is done and by its old partition until then.
func (l *Layout) Route(key string) (PartitionRef, *Partition) {
	if l == nil || l.PartitionCount <= 0 || len(l.Partitions) != l.PartitionCount {
		return PartitionRef{}, nil
	}
	h := hash.Sum(key)
	if r := l.Resharding; r != nil && r.PartitionCount > 0 && len(r.Partitions) == r.PartitionCount && len(r.Status) == r.PartitionCount {
		id := int(h % uint32(r.PartitionCount))
		if r.Status[id] == MigrationDone {
			return PartitionRef{Generation: r.Generation, ID: id}, &r.Partitions[id]
		}
	}
	id := int(h % uint32(l.PartitionCount))
	return PartitionRef{Generation: l.Generation, ID: id}, &l.Partitions[id]
}

// Owns reports whether ref is the partition currently responsible for key.
func (l *Layout) Owns(ref PartitionRef, key string) bool {
	got, p := l.Route(key)
	return p != nil && got == ref
}

func (l *Layout) Find(ref PartitionRef) *Partition {
	if l == nil {
		return nil
	}
	if ref.Generation == l.Generation && ref.ID >= 0 && ref.ID < len(l.Partitions) {
		return &l.Partitions[ref.ID]
	}
	if r := l.Resharding; r != nil && ref.Generation == r.Generation && ref.ID >= 0 && ref.ID < len(r.Partitions) {
		return &r.Partitions[ref.ID]
	}
	return nil
}

// Each calls fn for every partition of the current generation and, while resharding, the next one.
func (l *Layout) Each(fn func(ref PartitionRef, p *Partition)) {
	if l == nil {
		return
	}
	for i := range l.Partitions {
		fn(PartitionRef{Generation: l.Generation, ID: l.Partitions[i].ID}, &l.Partitions[i])
	}
	if r := l.Resharding; r != nil {
		for i := range r.Partitions {
			fn(PartitionRef{Generation: r.Generation, ID: r.Partitions[i].ID}, &r.Partitions[i])
		}
	}
}

func (l *Layout) Clone() *Layout {
	data, _ := json.Marshal(l)
	var out Layout
	_ = json.Unmarshal(data, &out)
	if out.Members == nil {
		out.Members = make(map[string]int64)
	}
	return &out
}

const (
	OpSet        = "set"
	OpDelete     = "delete"
	OpCheckpoint = "checkpoint"
)

type Operation struct {
	Type      string    `json:"type"` // "set", "delete" or "checkpoint"
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value,omitempty"`
	Timestamp time.Time `json:"ts"`
}

type LogEntry struct {
	SequenceNumber int64     `json:"seq"`
	Operation      Operation `json:"op"`
}

// Snapshot is a frozen copy of a partition (or of a key range of it) as of Seq.
type Snapshot struct {
	Generation  int64             `json:"generation"`
	PartitionID int               `json:"partition_id"`
	Epoch       int64             `json:"epoch"`
	Seq         int64             `json:"seq"`
	Data        map[string]string `json:"data"`
}

type ReplicateResponse struct {
	Epoch     int64      `json:"epoch"`
	LeaderSeq int64      `json:"leader_seq"`
	Entries   []LogEntry `json:"entries"`
}

// MigrationChunk is what an old partition hands over for one target partition while resharding:
// either a filtered snapshot or the filtered WAL entries after a given sequence number.
type MigrationChunk struct {
	Mode    string            `json:"mode"` // "snapshot" or "logs"
	Epoch   int64             `json:"epoch"`
	Seq     int64             `json:"seq"`
	Data    map[string]string `json:"data,omitempty"`
	Entries []LogEntry        `json:"entries,omitempty"`
}

type MigrationSource struct {
	PartitionID int    `json:"partition_id"`
	Epoch       int64  `json:"epoch"`
	Address     string `json:"address"`
}

type MigrationRequest struct {
	Target           PartitionRef      `json:"target"`
	TargetEpoch      int64             `json:"target_epoch"`
	TargetCount      int               `json:"target_count"`
	SourceGeneration int64             `json:"source_generation"`
	Sources          []MigrationSource `json:"sources"`
}

type MigrationResult struct {
	Keys     int `json:"keys"`
	Imported int `json:"imported"`
}

type PartitionStatus struct {
	Generation int64    `json:"generation"`
	ID         int      `json:"id"`
	Role       NodeRole `json:"role"`
	Epoch      int64    `json:"epoch"`
	DataEpoch  int64    `json:"data_epoch"`
	Seq        int64    `json:"seq"`
	Synced     bool     `json:"synced"`
	Sealed     bool     `json:"sealed"`
	Keys       int      `json:"keys"`
	MemTable   int      `json:"memtable"`
	Levels     int      `json:"levels"`
	WAL        int      `json:"wal"`
}

func (s PartitionStatus) Ref() PartitionRef {
	return PartitionRef{Generation: s.Generation, ID: s.ID}
}

type NodeStatus struct {
	Node
	LayoutVersion int64             `json:"layout_version"`
	Partitions    []PartitionStatus `json:"partitions"`
	Ops           map[string]int64  `json:"ops"`
}

func (s *NodeStatus) Partition(ref PartitionRef) (PartitionStatus, bool) {
	for _, p := range s.Partitions {
		if p.Ref() == ref {
			return p, true
		}
	}
	return PartitionStatus{}, false
}

func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func RemoveString(slice []string, s string) []string {
	out := slice[:0:0]
	for _, item := range slice {
		if item != s {
			out = append(out, item)
		}
	}
	return out
}
