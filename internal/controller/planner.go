package controller

import (
	"fmt"
	"sort"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

const (
	handoffTimeout   = 10 * time.Second
	joinLagThreshold = 64 // WAL entries a replica may trail the leader by and still count as in sync
	maxMovesPerRound = 2
)

// planInput is everything the planner looks at besides the layout itself.
type planInput struct {
	Config      model.Config
	Nodes       map[string]model.Node // live membership (etcd leases)
	Drained     map[string]bool
	Status      map[string]model.NodeStatus // what each node reported this round
	Now         time.Time
	AutoBalance bool
}

type planner struct {
	l          *model.Layout
	in         planInput
	eligible   []string        // members that may receive replicas (not drained), sorted
	isEligible map[string]bool //
	load       map[string]int  // replicas per eligible node, not counting ones being moved away
	events     []string
}

// plan moves the layout one step towards the desired state and returns what it changed.
// It is a pure function of its inputs; the controller runs it every round and writes the
// result back to etcd only if something changed.
func plan(l *model.Layout, in planInput) []string {
	p := &planner{l: l, in: in}
	p.bootstrap()
	p.membership()
	p.computeLoad()
	l.Each(p.failover)
	l.Each(p.promoteJoiners)
	l.Each(p.progressHandoff)
	l.Each(p.handoffEvictedLeader)
	l.Each(p.fixReplicaCount)
	p.startResharding()
	if in.AutoBalance && l.Resharding == nil {
		p.balanceReplicas()
		p.balanceLeaders()
	}
	return p.events
}

func (p *planner) logf(format string, args ...interface{}) {
	p.events = append(p.events, fmt.Sprintf(format, args...))
}

func (p *planner) bootstrap() {
	l := p.l
	if l.Members == nil {
		l.Members = make(map[string]int64)
	}
	if l.Generation == 0 {
		l.Generation = 1
	}
	if l.PartitionCount == 0 && p.in.Config.PartitionCount > 0 {
		l.PartitionCount = p.in.Config.PartitionCount
		l.Partitions = make([]model.Partition, l.PartitionCount)
		for i := range l.Partitions {
			l.Partitions[i] = model.Partition{ID: i, State: model.PartitionActive}
		}
		p.logf("initialized layout with %d partitions (replication factor %d)", l.PartitionCount, p.in.Config.ReplicationFactor)
	}
}

// membership reconciles the layout's member list with the live leases in etcd. A node whose
// lease is gone, or that came back with a new incarnation (restarted, so its memory is empty),
// is removed from every partition; the failover step below then replaces it.
func (p *planner) membership() {
	for _, id := range sortedKeys(p.l.Members) {
		node, alive := p.in.Nodes[id]
		if alive && node.Incarnation == p.l.Members[id] {
			continue
		}
		p.removeNode(id)
		delete(p.l.Members, id)
		if alive {
			p.logf("node %s restarted (new incarnation): its replicas are failed over and it rejoins empty", id)
		} else {
			p.logf("node %s left the cluster (etcd lease expired): failing over its partitions", id)
		}
	}
	for _, id := range sortedKeys(p.in.Nodes) {
		if _, ok := p.l.Members[id]; !ok {
			p.l.Members[id] = p.in.Nodes[id].Incarnation
			p.logf("node %s joined the cluster (%s)", id, p.in.Nodes[id].Address)
		}
	}
}

func (p *planner) removeNode(id string) {
	p.l.Each(func(_ model.PartitionRef, part *model.Partition) {
		if part.Leader == id {
			part.Leader = ""
		}
		part.Followers = model.RemoveString(part.Followers, id)
		part.Joining = model.RemoveString(part.Joining, id)
		part.Evict = model.RemoveString(part.Evict, id)
		if part.HandoffTo == id {
			clearHandoff(part)
		}
	})
}

func (p *planner) computeLoad() {
	p.isEligible = make(map[string]bool)
	p.load = make(map[string]int)
	for _, id := range sortedKeys(p.l.Members) {
		if !p.in.Drained[id] {
			p.eligible = append(p.eligible, id)
			p.isEligible[id] = true
			p.load[id] = 0
		}
	}
	p.l.Each(func(_ model.PartitionRef, part *model.Partition) {
		for _, h := range part.Holders() {
			if p.isEligible[h] && !p.evicted(h, part) {
				p.load[h]++
			}
		}
	})
}

func (p *planner) evicted(node string, part *model.Partition) bool {
	return p.in.Drained[node] || model.ContainsString(part.Evict, node)
}

func (p *planner) status(node string, ref model.PartitionRef) (model.PartitionStatus, bool) {
	st, ok := p.in.Status[node]
	if !ok {
		return model.PartitionStatus{}, false
	}
	return st.Partition(ref)
}

// failover gives a leaderless partition a new leader: the most up-to-date in-sync follower,
// else a synced joining replica, else (nothing left) the least loaded node, starting empty.
func (p *planner) failover(ref model.PartitionRef, part *model.Partition) {
	if part.Leader != "" {
		return
	}
	candidate, seq := p.mostUpToDate(ref, part.Followers, false)
	if candidate == "" {
		candidate, seq = p.mostUpToDate(ref, part.Joining, true)
	}
	switch {
	case candidate != "":
		p.logf("failover: %s is the new leader of %s (epoch %d, seq %d)", candidate, ref, part.Epoch+1, seq)
	default:
		candidate = p.leastLoaded(part)
		if candidate == "" {
			return
		}
		p.load[candidate]++
		if part.Epoch > 0 {
			p.logf("WARNING: partition %s lost every replica; %s now leads it empty (epoch %d)", ref, candidate, part.Epoch+1)
		} else {
			p.logf("partition %s: assigned leader %s", ref, candidate)
		}
	}
	part.Followers = model.RemoveString(part.Followers, candidate)
	part.Joining = model.RemoveString(part.Joining, candidate)
	part.Leader = candidate
	part.Epoch++
	clearHandoff(part)
}

// mostUpToDate picks the candidate with the newest data (data epoch, then sequence number),
// preferring nodes that are not being drained.
func (p *planner) mostUpToDate(ref model.PartitionRef, candidates []string, requireSynced bool) (string, int64) {
	best, found, bestEligible := "", false, false
	var bestSt model.PartitionStatus
	for _, c := range candidates {
		st, ok := p.status(c, ref)
		if requireSynced && (!ok || !st.Synced) {
			continue
		}
		eligible := p.isEligible[c]
		better := !found ||
			(eligible && !bestEligible) ||
			(eligible == bestEligible && (st.DataEpoch > bestSt.DataEpoch || (st.DataEpoch == bestSt.DataEpoch && st.Seq > bestSt.Seq)))
		if better {
			best, bestSt, bestEligible, found = c, st, eligible, true
		}
	}
	return best, bestSt.Seq
}

func (p *planner) leastLoaded(part *model.Partition) string {
	best := ""
	for _, id := range p.eligible {
		if part.Has(id) {
			continue
		}
		if best == "" || p.load[id] < p.load[best] {
			best = id
		}
	}
	return best
}

// promoteJoiners turns joining replicas into in-sync followers once they have loaded a
// snapshot from the current leader and are close enough to its WAL position.
func (p *planner) promoteJoiners(ref model.PartitionRef, part *model.Partition) {
	if part.Leader == "" || len(part.Joining) == 0 {
		return
	}
	leader, ok := p.status(part.Leader, ref)
	if !ok || leader.Role != model.NodeRoleLeader || leader.Epoch != part.Epoch {
		return
	}
	for _, j := range append([]string(nil), part.Joining...) {
		st, ok := p.status(j, ref)
		if !ok || !st.Synced || st.Role != model.NodeRoleFollower || st.DataEpoch != part.Epoch || leader.Seq-st.Seq > joinLagThreshold {
			continue
		}
		part.Joining = model.RemoveString(part.Joining, j)
		part.Followers = append(part.Followers, j)
		p.logf("partition %s: replica on %s caught up (seq %d/%d) and now serves reads", ref, j, st.Seq, leader.Seq)
	}
}

// progressHandoff completes a leadership transfer once the old leader has sealed the partition
// (so its sequence number is final) and the target has replicated everything up to it.
func (p *planner) progressHandoff(ref model.PartitionRef, part *model.Partition) {
	if part.State != model.PartitionHandoff {
		return
	}
	target := part.HandoffTo
	if part.Leader == "" || !model.ContainsString(part.Followers, target) {
		p.logf("partition %s: handoff to %s cancelled (it is no longer an in-sync replica)", ref, target)
		clearHandoff(part)
		return
	}
	if p.in.Now.Sub(time.UnixMilli(part.HandoffAt)) > handoffTimeout {
		p.logf("partition %s: handoff to %s timed out; %s keeps leading", ref, target, part.Leader)
		clearHandoff(part)
		return
	}
	leaderNode, ok := p.in.Status[part.Leader]
	if !ok || leaderNode.LayoutVersion < part.HandoffVersion {
		return
	}
	ls, ok := leaderNode.Partition(ref)
	if !ok || !ls.Sealed {
		return
	}
	ts, ok := p.status(target, ref)
	if !ok || ts.DataEpoch != part.Epoch || ts.Seq < ls.Seq {
		return
	}

	old := part.Leader
	part.Followers = model.RemoveString(part.Followers, target)
	part.Leader = target
	part.Epoch++
	clearHandoff(part)
	if p.evicted(old, part) {
		part.Evict = model.RemoveString(part.Evict, old)
	} else {
		part.Joining = append(part.Joining, old)
	}
	p.logf("partition %s: leadership handed off %s -> %s at seq %d (epoch %d); %s discards its copy", ref, old, target, ls.Seq, part.Epoch, old)
}

func (p *planner) handoffEvictedLeader(ref model.PartitionRef, part *model.Partition) {
	if part.Leader == "" || part.State == model.PartitionHandoff || !p.evicted(part.Leader, part) {
		return
	}
	var candidates []string
	for _, f := range part.Followers {
		if p.isEligible[f] && !p.evicted(f, part) {
			candidates = append(candidates, f)
		}
	}
	if target, _ := p.mostUpToDate(ref, candidates, true); target != "" {
		p.startHandoff(ref, part, target, fmt.Sprintf("%s is being moved off", part.Leader))
	}
}

func (p *planner) startHandoff(ref model.PartitionRef, part *model.Partition, target, reason string) {
	part.State = model.PartitionHandoff
	part.HandoffTo = target
	part.HandoffAt = p.in.Now.UnixMilli()
	part.HandoffVersion = p.l.Version
	p.logf("partition %s: handing leadership %s -> %s (%s); writes pause until it has caught up", ref, part.Leader, target, reason)
}

func clearHandoff(part *model.Partition) {
	part.State = model.PartitionActive
	part.HandoffTo = ""
	part.HandoffAt = 0
	part.HandoffVersion = 0
}

func (p *planner) desiredCopies() int {
	desired := p.in.Config.ReplicationFactor
	if desired < 1 {
		desired = 1
	}
	if desired > len(p.eligible) {
		desired = len(p.eligible)
	}
	return desired
}

// fixReplicaCount adds joining replicas until the partition has replication-factor copies and
// removes surplus ones — but only ever removes a copy once enough others are in sync.
func (p *planner) fixReplicaCount(ref model.PartitionRef, part *model.Partition) {
	desired := p.desiredCopies()
	if part.Leader == "" || desired == 0 {
		return
	}
	keep := func(n string) bool { return !p.evicted(n, part) }

	inSync, joining := 0, 0
	if keep(part.Leader) {
		inSync++
	}
	for _, f := range part.Followers {
		if keep(f) {
			inSync++
		}
	}
	for _, j := range part.Joining {
		if keep(j) {
			joining++
		}
	}

	for inSync+joining < desired {
		n := p.leastLoaded(part)
		if n == "" {
			break
		}
		part.Joining = append(part.Joining, n)
		joining++
		p.load[n]++
		p.logf("partition %s: adding a replica on %s", ref, n)
	}

	if inSync >= desired {
		for _, n := range append(append([]string(nil), part.Followers...), part.Joining...) {
			if keep(n) || n == part.HandoffTo {
				continue
			}
			part.Followers = model.RemoveString(part.Followers, n)
			part.Joining = model.RemoveString(part.Joining, n)
			p.logf("partition %s: removed the replica on %s", ref, n)
		}
		for inSync > desired {
			victim := ""
			for _, f := range part.Followers {
				if keep(f) && f != part.HandoffTo && (victim == "" || p.load[f] >= p.load[victim]) {
					victim = f
				}
			}
			if victim == "" {
				break
			}
			part.Followers = model.RemoveString(part.Followers, victim)
			inSync--
			p.load[victim]--
			p.logf("partition %s: removed surplus replica on %s", ref, victim)
		}
		for _, j := range append([]string(nil), part.Joining...) {
			if keep(j) {
				part.Joining = model.RemoveString(part.Joining, j)
				p.load[j]--
				p.logf("partition %s: cancelled joining replica on %s (enough in-sync copies)", ref, j)
			}
		}
	}

	var evict []string
	for _, n := range part.Evict {
		if part.Has(n) {
			evict = append(evict, n)
		}
	}
	part.Evict = evict
}

// startResharding enters the intermediate state of §13: the next generation's partitions are
// created (empty, with leaders and replicas) next to the current ones. The controller's
// resharding loop then migrates them one by one.
func (p *planner) startResharding() {
	l := p.l
	target := p.in.Config.PartitionCount
	if l.Resharding != nil || target <= 0 || target == l.PartitionCount || l.PartitionCount == 0 || len(p.eligible) == 0 {
		return
	}
	for i := range l.Partitions {
		if !l.Partitions[i].Writable() {
			return
		}
	}

	copies := p.desiredCopies()
	r := &model.Resharding{
		Generation:     l.Generation + 1,
		PartitionCount: target,
		Partitions:     make([]model.Partition, target),
		Status:         make([]model.MigrationStatus, target),
	}
	leaders := make(map[string]int)
	for i := range r.Partitions {
		part := &r.Partitions[i]
		*part = model.Partition{ID: i, Epoch: 1, State: model.PartitionActive}
		// Spread leaderships evenly up front; picking by total load alone keeps choosing the same
		// nodes as leaders and leaves the balancer a long series of handoffs afterwards.
		for _, id := range p.eligible {
			if part.Leader == "" || leaders[id] < leaders[part.Leader] ||
				(leaders[id] == leaders[part.Leader] && p.load[id] < p.load[part.Leader]) {
				part.Leader = id
			}
		}
		leaders[part.Leader]++
		p.load[part.Leader]++
		for len(part.Joining)+1 < copies {
			n := p.leastLoaded(part)
			if n == "" {
				break
			}
			part.Joining = append(part.Joining, n)
			p.load[n]++
		}
		r.Status[i] = model.MigrationPending
	}
	l.Resharding = r
	p.logf("resharding started: %d -> %d partitions (generation %d -> %d)", l.PartitionCount, target, l.Generation, r.Generation)
}

// balanceReplicas moves replicas from the busiest to the idlest node (e.g. after a node was
// added). A move adds the new copy first and evicts the old one only once the new one is in sync.
func (p *planner) balanceReplicas() {
	if len(p.eligible) < 2 {
		return
	}
	for moves := 0; moves < maxMovesPerRound; moves++ {
		hi, lo := p.eligible[0], p.eligible[0]
		for _, id := range p.eligible {
			if p.load[id] > p.load[hi] {
				hi = id
			}
			if p.load[id] < p.load[lo] {
				lo = id
			}
		}
		if p.load[hi]-p.load[lo] < 2 || !p.moveReplica(hi, lo) {
			return
		}
	}
}

func (p *planner) moveReplica(from, to string) bool {
	// Prefer partitions where `from` is only a follower, so no leadership has to move.
	for pass := 0; pass < 2; pass++ {
		for i := range p.l.Partitions {
			part := &p.l.Partitions[i]
			if part.Pinned || part.State != model.PartitionActive || part.Leader == "" || len(part.Joining) > 0 || len(part.Evict) > 0 || part.Has(to) {
				continue
			}
			if (pass == 0 && !model.ContainsString(part.Followers, from)) || (pass == 1 && part.Leader != from) {
				continue
			}
			part.Joining = append(part.Joining, to)
			part.Evict = append(part.Evict, from)
			p.load[to]++
			p.load[from]--
			p.logf("rebalance: moving the replica of %s from %s to %s", model.PartitionRef{Generation: p.l.Generation, ID: part.ID}, from, to)
			return true
		}
	}
	return false
}

// balanceLeaders spreads leadership (and therefore write load) evenly, one handoff at a time.
func (p *planner) balanceLeaders() {
	leaders := make(map[string]int)
	for _, id := range p.eligible {
		leaders[id] = 0
	}
	for i := range p.l.Partitions {
		if p.l.Partitions[i].State == model.PartitionHandoff {
			return
		}
		if _, ok := leaders[p.l.Partitions[i].Leader]; ok {
			leaders[p.l.Partitions[i].Leader]++
		}
	}

	for i := range p.l.Partitions {
		part := &p.l.Partitions[i]
		ref := model.PartitionRef{Generation: p.l.Generation, ID: part.ID}
		current, ok := leaders[part.Leader]
		if !ok || part.Pinned || part.State != model.PartitionActive || len(part.Evict) > 0 {
			continue
		}
		ls, ok := p.status(part.Leader, ref)
		if !ok {
			continue
		}
		for _, f := range part.Followers {
			count, ok := leaders[f]
			if !ok || current-count < 2 {
				continue
			}
			fs, ok := p.status(f, ref)
			if !ok || fs.DataEpoch != part.Epoch || ls.Seq-fs.Seq > joinLagThreshold {
				continue
			}
			p.startHandoff(ref, part, f, fmt.Sprintf("balancing leaders: %s leads %d partitions, %s leads %d", part.Leader, current, f, count))
			return
		}
	}
}

// unstableReason explains why the cluster has not converged yet ("" once it has).
func unstableReason(l *model.Layout, cfg model.Config, drained map[string]bool, autoBalance bool) string {
	if l == nil || l.PartitionCount == 0 {
		return "no layout yet"
	}
	if r := l.Resharding; r != nil {
		done := 0
		for _, s := range r.Status {
			if s == model.MigrationDone {
				done++
			}
		}
		return fmt.Sprintf("resharding %d -> %d partitions (%d/%d migrated)", l.PartitionCount, r.PartitionCount, done, r.PartitionCount)
	}
	if cfg.PartitionCount > 0 && cfg.PartitionCount != l.PartitionCount {
		return fmt.Sprintf("partition count change to %d pending", cfg.PartitionCount)
	}
	eligible := 0
	for id := range l.Members {
		if !drained[id] {
			eligible++
		}
	}
	desired := cfg.ReplicationFactor
	if desired < 1 {
		desired = 1
	}
	if desired > eligible {
		desired = eligible
	}
	for i := range l.Partitions {
		part := &l.Partitions[i]
		ref := model.PartitionRef{Generation: l.Generation, ID: part.ID}
		switch {
		case part.Leader == "":
			return fmt.Sprintf("partition %s has no leader", ref)
		case part.State == model.PartitionHandoff:
			return fmt.Sprintf("partition %s is handing off leadership", ref)
		case len(part.Joining) > 0:
			return fmt.Sprintf("partition %s has replicas still joining", ref)
		case len(part.Evict) > 0:
			return fmt.Sprintf("partition %s has a replica move pending", ref)
		case 1+len(part.Followers) != desired:
			return fmt.Sprintf("partition %s has %d/%d in-sync copies", ref, 1+len(part.Followers), desired)
		}
		for _, h := range part.Holders() {
			if drained[h] {
				return fmt.Sprintf("drained node %s still holds %s", h, ref)
			}
		}
	}
	if autoBalance {
		replicas, leaders := make(map[string]int), make(map[string]int)
		for id := range l.Members {
			if !drained[id] {
				replicas[id], leaders[id] = 0, 0
			}
		}
		for _, part := range l.Partitions {
			leaders[part.Leader]++
			for _, h := range part.Holders() {
				replicas[h]++
			}
		}
		if spread(replicas) >= 2 {
			return fmt.Sprintf("rebalancing replicas %v", replicas)
		}
		if spread(leaders) >= 2 {
			return fmt.Sprintf("rebalancing leaders %v", leaders)
		}
	}
	return ""
}

func spread(counts map[string]int) int {
	lo, hi := 0, 0
	first := true
	for _, v := range counts {
		if first || v < lo {
			lo = v
		}
		if first || v > hi {
			hi = v
		}
		first = false
	}
	return hi - lo
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
