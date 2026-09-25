package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
)

// cluster simulates nodes that apply every layout instantly and are always fully caught up.
type cluster struct {
	t      *testing.T
	layout *model.Layout
	in     planInput
	seq    map[string]map[model.PartitionRef]int64 // per-node overrides of reported seq
}

func newCluster(t *testing.T, partitions, rf int, nodes ...string) *cluster {
	c := &cluster{
		t:      t,
		layout: &model.Layout{Members: map[string]int64{}},
		in: planInput{
			Config:      model.Config{PartitionCount: partitions, ReplicationFactor: rf},
			Nodes:       map[string]model.Node{},
			Drained:     map[string]bool{},
			AutoBalance: true,
		},
		seq: map[string]map[model.PartitionRef]int64{},
	}
	for _, n := range nodes {
		c.addNode(n)
	}
	return c
}

func (c *cluster) addNode(id string) {
	c.in.Nodes[id] = model.Node{ID: id, Address: id + ":7000", Incarnation: time.Now().UnixNano()}
}

func (c *cluster) statuses() map[string]model.NodeStatus {
	out := make(map[string]model.NodeStatus)
	for id, n := range c.in.Nodes {
		st := model.NodeStatus{Node: n, LayoutVersion: c.layout.Version}
		if c.layout.Members[id] == n.Incarnation {
			c.layout.Each(func(ref model.PartitionRef, p *model.Partition) {
				if !p.Has(id) {
					return
				}
				ps := model.PartitionStatus{Generation: ref.Generation, ID: ref.ID, Epoch: p.Epoch, DataEpoch: p.Epoch, Seq: 100, Synced: true, Role: model.NodeRoleFollower}
				if p.Leader == id {
					ps.Role = model.NodeRoleLeader
					ps.Sealed = p.State == model.PartitionHandoff
				}
				if s, ok := c.seq[id][ref]; ok {
					ps.Seq = s
				}
				st.Partitions = append(st.Partitions, ps)
			})
		}
		out[id] = st
	}
	return out
}

func (c *cluster) round() []string {
	c.in.Status = c.statuses()
	c.in.Now = time.Now()
	c.layout.Version++
	return plan(c.layout, c.in)
}

func (c *cluster) converge() {
	c.t.Helper()
	for i := 0; i < 100; i++ {
		if events := c.round(); len(events) == 0 {
			if reason := unstableReason(c.layout, c.in.Config, c.in.Drained, c.in.AutoBalance); reason != "" {
				c.t.Fatalf("no more changes but not stable: %s", reason)
			}
			return
		}
	}
	c.t.Fatal("layout did not converge")
}

func (c *cluster) load() (replicas, leaders map[string]int) {
	replicas, leaders = map[string]int{}, map[string]int{}
	for id := range c.in.Nodes {
		if !c.in.Drained[id] {
			replicas[id], leaders[id] = 0, 0
		}
	}
	for _, p := range c.layout.Partitions {
		leaders[p.Leader]++
		for _, h := range p.Holders() {
			replicas[h]++
		}
	}
	return
}

func (c *cluster) assertHealthy(rf int) {
	c.t.Helper()
	for _, p := range c.layout.Partitions {
		if p.Leader == "" || 1+len(p.Followers) != rf || len(p.Joining) != 0 {
			c.t.Fatalf("partition %d unhealthy: %+v", p.ID, p)
		}
		for _, h := range p.Holders() {
			if _, alive := c.in.Nodes[h]; !alive {
				c.t.Fatalf("partition %d still assigned to dead node %s", p.ID, h)
			}
		}
	}
	replicas, leaders := c.load()
	if spread(replicas) > 1 || spread(leaders) > 1 {
		c.t.Fatalf("unbalanced: replicas %v leaders %v", replicas, leaders)
	}
}

func TestBootstrapSpreadsPartitions(t *testing.T) {
	c := newCluster(t, 8, 2, "n1", "n2", "n3")
	c.converge()
	c.assertHealthy(2)
}

func TestNodeFailureFailsOver(t *testing.T) {
	c := newCluster(t, 8, 2, "n1", "n2", "n3")
	c.converge()
	epochs := map[int]int64{}
	led := map[int]bool{}
	for _, p := range c.layout.Partitions {
		epochs[p.ID], led[p.ID] = p.Epoch, p.Leader == "n1"
	}

	delete(c.in.Nodes, "n1") // lease expired
	c.converge()
	c.assertHealthy(2)
	for _, p := range c.layout.Partitions {
		if led[p.ID] && p.Epoch <= epochs[p.ID] {
			t.Fatalf("partition %d lost its leader but the epoch did not change", p.ID)
		}
	}
}

func TestRestartedNodeIsTreatedAsNew(t *testing.T) {
	c := newCluster(t, 6, 3, "n1", "n2", "n3")
	c.converge()

	c.addNode("n1") // same id, new incarnation: its memory is empty
	c.round()
	for _, p := range c.layout.Partitions {
		if p.Leader == "n1" || model.ContainsString(p.Followers, "n1") {
			t.Fatalf("restarted node kept an in-sync role in partition %d: %+v", p.ID, p)
		}
	}
	c.converge()
	c.assertHealthy(3)
}

func TestFailoverPicksMostUpToDateFollower(t *testing.T) {
	c := newCluster(t, 1, 3, "n1", "n2", "n3")
	c.converge()
	p := c.layout.Partitions[0]
	ref := model.PartitionRef{Generation: c.layout.Generation, ID: 0}
	behind, ahead := p.Followers[0], p.Followers[1]
	c.seq[behind] = map[model.PartitionRef]int64{ref: 50}
	c.seq[ahead] = map[model.PartitionRef]int64{ref: 90}

	delete(c.in.Nodes, p.Leader)
	c.round()
	if got := c.layout.Partitions[0].Leader; got != ahead {
		t.Fatalf("new leader = %s, want the most up-to-date follower %s", got, ahead)
	}
}

func TestAddingNodeRebalances(t *testing.T) {
	c := newCluster(t, 12, 2, "n1", "n2", "n3")
	c.converge()
	c.addNode("n4")
	c.converge()
	c.assertHealthy(2)
	if replicas, _ := c.load(); replicas["n4"] < 5 {
		t.Fatalf("new node got only %d replicas", replicas["n4"])
	}
}

func TestDrainEmptiesNode(t *testing.T) {
	c := newCluster(t, 8, 2, "n1", "n2", "n3", "n4")
	c.converge()
	c.in.Drained["n2"] = true
	c.converge()
	for _, p := range c.layout.Partitions {
		if p.Has("n2") {
			t.Fatalf("drained node still holds partition %d", p.ID)
		}
	}
	c.assertHealthy(2)
}

func TestReplicationFactorChanges(t *testing.T) {
	c := newCluster(t, 8, 2, "n1", "n2", "n3", "n4")
	c.converge()
	c.in.Config.ReplicationFactor = 3
	c.converge()
	c.assertHealthy(3)
	c.in.Config.ReplicationFactor = 1
	c.converge()
	c.assertHealthy(1)
}

func TestManualMoveOfLeader(t *testing.T) {
	c := newCluster(t, 4, 2, "n1", "n2", "n3")
	c.in.AutoBalance = false
	c.converge()
	p := &c.layout.Partitions[0]
	from := p.Leader
	to := ""
	for _, n := range []string{"n1", "n2", "n3"} {
		if !p.Has(n) {
			to = n
		}
	}
	p.Joining = append(p.Joining, to)
	p.Evict = append(p.Evict, from)
	c.converge()
	p = &c.layout.Partitions[0]
	if p.Has(from) || !p.Has(to) {
		t.Fatalf("move %s -> %s not applied: %+v", from, to, p)
	}
}

func TestReshardingStartsNextGeneration(t *testing.T) {
	c := newCluster(t, 4, 2, "n1", "n2", "n3")
	c.converge()
	c.in.Config.PartitionCount = 8
	c.round()
	r := c.layout.Resharding
	if r == nil || r.PartitionCount != 8 || r.Generation != c.layout.Generation+1 {
		t.Fatalf("resharding not started: %+v", r)
	}
	for i, p := range r.Partitions {
		if p.Leader == "" || r.Status[i] != model.MigrationPending {
			t.Fatalf("new partition %d not prepared: %+v", i, p)
		}
	}
	// Until a partition is migrated, its keys still route to the old generation.
	for i := 0; i < 100; i++ {
		if ref, _ := c.layout.Route(fmt.Sprint("k", i)); ref.Generation != c.layout.Generation {
			t.Fatal("key routed to an unmigrated partition")
		}
	}
	r.Status[3] = model.MigrationDone
	for i := 0; i < 200; i++ {
		k := fmt.Sprint("k", i)
		ref, _ := c.layout.Route(k)
		moved := ref.Generation == r.Generation
		if moved != (hash.GetPartitionID(k, 8) == 3) {
			t.Fatalf("key %s routed to %s", k, ref)
		}
	}
}

func TestBalancerLeavesPinnedPartitionsAlone(t *testing.T) {
	c := newCluster(t, 6, 2, "n1", "n2", "n3")
	c.converge()
	// Pile every leadership of the partitions n1 follows onto n1, by hand.
	for i := range c.layout.Partitions {
		p := &c.layout.Partitions[i]
		if model.ContainsString(p.Followers, "n1") {
			p.Followers = append(model.RemoveString(p.Followers, "n1"), p.Leader)
			p.Leader = "n1"
			p.Epoch++
			p.Pinned = true
		}
	}
	before := map[int]string{}
	for _, p := range c.layout.Partitions {
		before[p.ID] = p.Leader
	}
	for i := 0; i < 20; i++ {
		c.round()
	}
	for _, p := range c.layout.Partitions {
		if p.Pinned && p.Leader != before[p.ID] {
			t.Fatalf("balancer moved pinned partition %d from %s to %s", p.ID, before[p.ID], p.Leader)
		}
	}
}
