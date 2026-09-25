package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sharif-go-lab/SliceDB/internal/metrics"
	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/partition"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
	"github.com/sharif-go-lab/SliceDB/pkg/network"
)

const (
	replicationBatch = 2000
	replicationWait  = time.Second
	// A freeze only has to outlive the gap between a finished pull and the controller marking the
	// target partition done; it expires on its own if the migration is abandoned.
	freezeTTL = 30 * time.Second
)

type Options struct {
	ID        string
	Listen    string // address the HTTP server binds to, e.g. ":7000"
	Advertise string // address other components use to reach this node, e.g. "node1:7000"
	Etcd      []string
	LeaseTTL  int64
	Partition partition.Options
}

// replica is a partition held by this node plus, for followers, its replication loop.
type replica struct {
	part   *partition.Partition
	cancel context.CancelFunc
	done   chan struct{}
}

func (r *replica) stopFollowing() {
	if r.cancel != nil {
		r.cancel()
		<-r.done
		r.cancel, r.done = nil, nil
	}
}

// Node is a database node. It learns what to hold purely from the layout the controller
// publishes in etcd, and it announces itself through a lease-bound key in etcd.
type Node struct {
	opts        Options
	incarnation atomic.Int64
	registry    *registry.EtcdRegistry
	client      *network.Client // short requests
	longClient  *network.Client // replication long-polls, snapshots and migrations
	nodes       *registry.NodeWatcher
	layout      *registry.LayoutWatcher
	observed    atomic.Int64 // layout version fully applied by reconcile
	lease       atomic.Int64 // current membership lease

	reconcileMu sync.Mutex
	mu          sync.RWMutex
	replicas    map[model.PartitionRef]*replica

	ops *metrics.Counters
}

func NewNode(opts Options) (*Node, error) {
	reg, err := registry.NewEtcdRegistry(opts.Etcd)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 5
	}

	n := &Node{
		opts:       opts,
		registry:   reg,
		client:     network.NewClientWithTimeout(3 * time.Second),
		longClient: network.NewClientWithTimeout(2 * time.Minute),
		replicas:   make(map[model.PartitionRef]*replica),
		ops:        metrics.NewCounters(),
	}
	n.incarnation.Store(time.Now().UnixNano())
	return n, nil
}

// Start serves until ctx is cancelled (SIGTERM), then revokes the node's lease so the controller
// fails its partitions over immediately instead of waiting for the TTL.
func (n *Node) Start(ctx context.Context) error {
	ready := make(chan struct{})
	n.nodes = n.registry.WatchNodes(ctx, nil)
	n.layout = n.registry.WatchLayout(ctx, func(l *model.Layout) {
		<-ready
		n.reconcile(l)
	})
	close(ready)
	go n.keepMembership(ctx)
	go n.logStats(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/set", n.handleSet)
	mux.HandleFunc("/get", n.handleGet)
	mux.HandleFunc("/delete", n.handleDelete)
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/status", n.handleStatus)
	mux.HandleFunc("/replicate", n.handleReplicate)
	mux.HandleFunc("/snapshot", n.handleSnapshot)
	mux.HandleFunc("/migrate/export", n.handleMigrateExport)
	mux.HandleFunc("/migrate/pull", n.handleMigratePull)

	server := &http.Server{Addr: n.opts.Listen, Handler: mux}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		log.Printf("Node %s: shutting down; revoking etcd lease so partitions fail over now", n.opts.ID)
		n.registry.Revoke(clientv3.LeaseID(n.lease.Load()))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("Node %s starting on %s (advertised as %s)", n.opts.ID, n.opts.Listen, n.opts.Advertise)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-stopped
	return nil
}

// keepMembership keeps the node's lease-bound key in etcd. If the lease is ever lost, the
// controller has already failed our partitions over, so everything local is dropped and the
// node rejoins under a new incarnation (and will be handed partitions again, starting empty).
func (n *Node) keepMembership(ctx context.Context) {
	for ctx.Err() == nil {
		self := n.self()
		leaseID, lost, err := n.registry.RegisterNode(ctx, self, n.opts.LeaseTTL)
		if err != nil {
			log.Printf("Node %s: registering in etcd failed: %v", n.opts.ID, err)
			pause(ctx, time.Second)
			continue
		}
		n.lease.Store(int64(leaseID))
		log.Printf("Node %s: registered in etcd (incarnation %d, lease ttl %ds)", n.opts.ID, self.Incarnation, n.opts.LeaseTTL)
		<-lost
		if ctx.Err() != nil {
			return
		}

		log.Printf("Node %s: etcd lease lost; dropping local partitions and rejoining as a new incarnation", n.opts.ID)
		n.incarnation.Store(time.Now().UnixNano())
		n.reconcile(n.layout.Load())
	}
}

func (n *Node) self() model.Node {
	return model.Node{ID: n.opts.ID, Address: n.opts.Advertise, Incarnation: n.incarnation.Load()}
}

// reconcile makes the local partitions match the layout: create, promote, demote or drop them.
// Assignments are only honored once the controller has acknowledged this incarnation, so a
// freshly restarted node never believes it still leads a partition whose data it has lost.
func (n *Node) reconcile(l *model.Layout) {
	n.reconcileMu.Lock()
	defer n.reconcileMu.Unlock()

	desired := make(map[model.PartitionRef]*model.Partition)
	if l != nil && l.Members[n.opts.ID] == n.incarnation.Load() {
		l.Each(func(ref model.PartitionRef, p *model.Partition) {
			if p.Has(n.opts.ID) {
				desired[ref] = p
			}
		})
	}

	n.mu.Lock()
	for ref, r := range n.replicas {
		if _, ok := desired[ref]; !ok {
			r.stopFollowing()
			delete(n.replicas, ref)
			log.Printf("Node %s: dropped partition %s", n.opts.ID, ref)
		}
	}

	for ref, p := range desired {
		r, exists := n.replicas[ref]
		if !exists {
			r = &replica{part: partition.NewPartition(ref, n.opts.Partition)}
			n.replicas[ref] = r
		}

		if p.Leader == n.opts.ID {
			r.stopFollowing()
			switch {
			case !exists:
				r.part.LeadFresh(p.Epoch)
				log.Printf("Node %s: leading new partition %s (epoch %d)", n.opts.ID, ref, p.Epoch)
			case r.part.Role() != model.NodeRoleLeader || r.part.Epoch() != p.Epoch:
				r.part.Promote(p.Epoch)
				log.Printf("Node %s: promoted to leader of %s (epoch %d, seq %d)", n.opts.ID, ref, p.Epoch, r.part.SequenceNumber())
			}
			r.part.Seal(p.State == model.PartitionHandoff)
			continue
		}

		if r.part.Follow(p.Epoch) {
			log.Printf("Node %s: no longer leader of %s; discarding local data and resyncing from %s", n.opts.ID, ref, p.Leader)
		} else if !exists {
			log.Printf("Node %s: replicating partition %s from %s", n.opts.ID, ref, p.Leader)
		}
		if r.cancel == nil {
			n.startFollowing(ref, r)
		}
	}
	n.mu.Unlock()

	if l != nil {
		n.observed.Store(l.Version)
	}
}

func (n *Node) startFollowing(ref model.PartitionRef, r *replica) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.cancel, r.done = cancel, done
	go func() {
		defer close(done)
		n.follow(ctx, ref, r.part)
	}()
}

// follow keeps a follower in sync: load a snapshot from the leader first, then long-poll the
// leader's WAL for new entries. Epoch changes, truncated logs or divergence make the leader
// answer 410, after which the follower drops its data and loads a new snapshot.
func (n *Node) follow(ctx context.Context, ref model.PartitionRef, part *partition.Partition) {
	var lastErr string
	report := func(format string, args ...interface{}) {
		msg := fmt.Sprintf(format, args...)
		if msg != lastErr {
			log.Print(msg)
			lastErr = msg
		}
	}

	for ctx.Err() == nil {
		leaderID, leaderAddr, ok := n.leaderOf(ref)
		if !ok {
			pause(ctx, 300*time.Millisecond)
			continue
		}
		epoch := part.Epoch()

		if !part.Synced() {
			var snap model.Snapshot
			url := fmt.Sprintf("http://%s/snapshot?gen=%d&pid=%d&epoch=%d", leaderAddr, ref.Generation, ref.ID, epoch)
			if err := n.longClient.Do(ctx, http.MethodGet, url, nil, &snap); err != nil {
				if ctx.Err() == nil {
					report("Node %s: snapshot of %s from %s failed: %v", n.opts.ID, ref, leaderID, err)
				}
				pause(ctx, 500*time.Millisecond)
				continue
			}
			if err := part.LoadSnapshot(snap); err != nil {
				report("Node %s: loading snapshot of %s failed: %v", n.opts.ID, ref, err)
				continue
			}
			log.Printf("Node %s: %s loaded snapshot from %s (seq %d, %d keys)", n.opts.ID, ref, leaderID, snap.Seq, len(snap.Data))
			lastErr = ""
			continue
		}

		var resp model.ReplicateResponse
		url := fmt.Sprintf("http://%s/replicate?gen=%d&pid=%d&epoch=%d&data_epoch=%d&after=%d&wait_ms=%d",
			leaderAddr, ref.Generation, ref.ID, epoch, part.DataEpoch(), part.SequenceNumber(), replicationWait.Milliseconds())
		if err := n.longClient.Do(ctx, http.MethodGet, url, nil, &resp); err != nil {
			if ctx.Err() != nil {
				return
			}
			if network.StatusCode(err) == http.StatusGone {
				log.Printf("Node %s: %s cannot continue from seq %d with %s; reloading a snapshot", n.opts.ID, ref, part.SequenceNumber(), leaderID)
				part.Reset()
				continue
			}
			report("Node %s: replicating %s from %s failed: %v", n.opts.ID, ref, leaderID, err)
			pause(ctx, 300*time.Millisecond)
			continue
		}
		if err := part.ApplyLogEntries(resp.Entries, resp.Epoch); err != nil {
			log.Printf("Node %s: applying log of %s failed (%v); reloading a snapshot", n.opts.ID, ref, err)
			part.Reset()
			continue
		}
		n.ops.Add("replicated", int64(len(resp.Entries)))
		lastErr = ""
	}
}

func (n *Node) leaderOf(ref model.PartitionRef) (string, string, bool) {
	p := n.layout.Load().Find(ref)
	if p == nil || p.Leader == "" || p.Leader == n.opts.ID {
		return "", "", false
	}
	node, ok := n.nodes.Get(p.Leader)
	if !ok {
		return "", "", false
	}
	return node.ID, node.Address, true
}

func (n *Node) lookup(ref model.PartitionRef) *partition.Partition {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if r, ok := n.replicas[ref]; ok {
		return r.part
	}
	return nil
}

// requestError carries the HTTP status to answer with.
type requestError struct {
	code int
	msg  string
}

func (e *requestError) Error() string { return e.msg }

func fail(w http.ResponseWriter, err error) {
	var re *requestError
	switch {
	case errors.As(err, &re):
		http.Error(w, re.msg, re.code)
	case errors.Is(err, partition.ErrNotLeader), errors.Is(err, partition.ErrNotFollower):
		http.Error(w, err.Error(), http.StatusMisdirectedRequest)
	case errors.Is(err, partition.ErrSealed), errors.Is(err, partition.ErrFrozen), errors.Is(err, partition.ErrNotSynced):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func refFromQuery(r *http.Request) (model.PartitionRef, bool) {
	q := r.URL.Query()
	gen, err1 := strconv.ParseInt(q.Get("gen"), 10, 64)
	pid, err2 := strconv.Atoi(q.Get("pid"))
	if err1 != nil || err2 != nil {
		return model.PartitionRef{}, false
	}
	return model.PartitionRef{Generation: gen, ID: pid}, true
}

// target resolves the partition a data request is for and checks that this node may serve it.
// The load balancer passes the partition it routed to; the node double-checks against its own
// view of the layout so a request routed with a stale table is bounced (421) and retried.
func (n *Node) target(r *http.Request, key string, write bool) (*partition.Partition, error) {
	l := n.layout.Load()
	if l == nil {
		return nil, &requestError{http.StatusServiceUnavailable, "no layout yet"}
	}
	ref, ok := refFromQuery(r)
	if !ok {
		ref, _ = l.Route(key)
	}
	if !l.Owns(ref, key) {
		return nil, &requestError{http.StatusMisdirectedRequest, fmt.Sprintf("key does not belong to %s in layout v%d", ref, l.Version)}
	}
	part := n.lookup(ref)
	if part == nil {
		return nil, &requestError{http.StatusMisdirectedRequest, fmt.Sprintf("partition %s is not on node %s", ref, n.opts.ID)}
	}
	if write {
		if p := l.Find(ref); p == nil || p.Leader != n.opts.ID {
			return nil, &requestError{http.StatusMisdirectedRequest, fmt.Sprintf("node %s is not the leader of %s", n.opts.ID, ref)}
		}
	} else if part.Role() != model.NodeRoleLeader && !part.Synced() {
		return nil, &requestError{http.StatusServiceUnavailable, fmt.Sprintf("replica of %s is still syncing", ref)}
	}
	return part, nil
}

func (n *Node) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil || data.Key == "" {
		http.Error(w, "body must be {\"key\": ..., \"value\": ...}", http.StatusBadRequest)
		return
	}

	part, err := n.target(r, data.Key, true)
	if err != nil {
		n.ops.Inc("set.rejected")
		fail(w, err)
		return
	}
	entry, err := part.Set(data.Key, data.Value)
	if err != nil {
		n.ops.Inc("set.rejected")
		fail(w, err)
		return
	}
	n.ops.Inc("set")
	writeJSON(w, map[string]interface{}{"seq": entry.SequenceNumber})
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter required", http.StatusBadRequest)
		return
	}

	part, err := n.target(r, key, false)
	if err != nil {
		n.ops.Inc("get.rejected")
		fail(w, err)
		return
	}
	n.ops.Inc("get")
	value, exists := part.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]string{"value": value})
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil || data.Key == "" {
		http.Error(w, "body must be {\"key\": ...}", http.StatusBadRequest)
		return
	}

	part, err := n.target(r, data.Key, true)
	if err != nil {
		n.ops.Inc("delete.rejected")
		fail(w, err)
		return
	}
	entry, err := part.Delete(data.Key)
	if err != nil {
		n.ops.Inc("delete.rejected")
		fail(w, err)
		return
	}
	n.ops.Inc("delete")
	writeJSON(w, map[string]interface{}{"seq": entry.SequenceNumber})
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "healthy", "id": n.opts.ID})
}

func (n *Node) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, n.status())
}

func (n *Node) status() model.NodeStatus {
	st := model.NodeStatus{
		Node:          n.self(),
		LayoutVersion: n.observed.Load(),
		Ops:           n.ops.Snapshot(),
	}
	n.mu.RLock()
	for _, r := range n.replicas {
		st.Partitions = append(st.Partitions, r.part.Status())
	}
	n.mu.RUnlock()
	sort.Slice(st.Partitions, func(i, j int) bool {
		a, b := st.Partitions[i], st.Partitions[j]
		if a.Generation != b.Generation {
			return a.Generation < b.Generation
		}
		return a.ID < b.ID
	})
	return st
}

// handleReplicate serves WAL entries to a follower, holding the request open for up to
// wait_ms when there is nothing new, so replication behaves like a stream.
func (n *Node) handleReplicate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ref, ok := refFromQuery(r)
	epoch, err1 := strconv.ParseInt(q.Get("epoch"), 10, 64)
	dataEpoch, err2 := strconv.ParseInt(q.Get("data_epoch"), 10, 64)
	after, err3 := strconv.ParseInt(q.Get("after"), 10, 64)
	waitMs, _ := strconv.Atoi(q.Get("wait_ms"))
	if !ok || err1 != nil || err2 != nil || err3 != nil {
		http.Error(w, "gen, pid, epoch, data_epoch and after are required", http.StatusBadRequest)
		return
	}

	part := n.lookup(ref)
	if part == nil {
		http.Error(w, fmt.Sprintf("partition %s is not on this node", ref), http.StatusNotFound)
		return
	}
	if part.Role() != model.NodeRoleLeader || part.Epoch() != epoch {
		http.Error(w, fmt.Sprintf("not the leader of %s for epoch %d", ref, epoch), http.StatusConflict)
		return
	}
	if !part.CanServe(dataEpoch, after) {
		http.Error(w, "follower must reload a snapshot", http.StatusGone)
		return
	}

	changed := part.Changed()
	entries, ok := part.GetLogsAfter(after, replicationBatch)
	if !ok {
		http.Error(w, "log truncated; follower must reload a snapshot", http.StatusGone)
		return
	}
	if len(entries) == 0 && waitMs > 0 {
		timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
		select {
		case <-changed:
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
		timer.Stop()
		if part.Role() != model.NodeRoleLeader || part.Epoch() != epoch {
			http.Error(w, fmt.Sprintf("no longer the leader of %s for epoch %d", ref, epoch), http.StatusConflict)
			return
		}
		if entries, ok = part.GetLogsAfter(after, replicationBatch); !ok {
			http.Error(w, "log truncated; follower must reload a snapshot", http.StatusGone)
			return
		}
	}

	writeJSON(w, model.ReplicateResponse{Epoch: epoch, LeaderSeq: part.SequenceNumber(), Entries: entries})
}

func (n *Node) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	ref, ok := refFromQuery(r)
	if !ok {
		http.Error(w, "gen and pid are required", http.StatusBadRequest)
		return
	}
	part := n.lookup(ref)
	if part == nil {
		http.Error(w, fmt.Sprintf("partition %s is not on this node", ref), http.StatusNotFound)
		return
	}
	if e := r.URL.Query().Get("epoch"); e != "" && e != strconv.FormatInt(part.Epoch(), 10) {
		http.Error(w, fmt.Sprintf("not the leader of %s for epoch %s", ref, e), http.StatusConflict)
		return
	}

	start := time.Now()
	snap, err := part.Snapshot(nil)
	if err != nil {
		fail(w, err)
		return
	}
	n.ops.Inc("snapshots_served")
	log.Printf("Node %s: serving snapshot of %s (seq %d, %d keys, built in %s)", n.opts.ID, ref, snap.Seq, len(snap.Data), time.Since(start).Round(time.Microsecond))
	writeJSON(w, snap)
}

// handleMigrateExport runs on the leader of an old-generation partition while resharding and
// hands over the keys that belong to one partition of the new generation.
func (n *Node) handleMigrateExport(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ref, ok := refFromQuery(r)
	targetCount, err1 := strconv.Atoi(q.Get("target_count"))
	targetID, err2 := strconv.Atoi(q.Get("target_id"))
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	freezeKeys := q.Get("freeze") == "1"
	if !ok || err1 != nil || err2 != nil || targetCount <= 0 {
		http.Error(w, "gen, pid, target_count and target_id are required", http.StatusBadRequest)
		return
	}
	part := n.lookup(ref)
	if part == nil {
		http.Error(w, fmt.Sprintf("partition %s is not on this node", ref), http.StatusNotFound)
		return
	}

	match := func(key string) bool { return hash.GetPartitionID(key, targetCount) == targetID }
	tag := fmt.Sprintf("%d/%d", targetCount, targetID)
	chunk, err := part.Export(tag, match, freezeKeys, after, freezeTTL)
	if err != nil {
		fail(w, err)
		return
	}
	if freezeKeys {
		log.Printf("Node %s: froze keys of %s that move to partition %d/%d (handing over %d log entries)", n.opts.ID, ref, targetID, targetCount, len(chunk.Entries))
	}
	writeJSON(w, chunk)
}

// handleMigratePull runs on the leader of a new-generation partition: it copies its keys from
// every old partition that may hold some, first from snapshots while writes continue, then —
// after freezing those keys on the old leaders — the few writes that happened in between.
func (n *Node) handleMigratePull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req model.MigrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	part := n.lookup(req.Target)
	if part == nil || part.Role() != model.NodeRoleLeader || part.Epoch() != req.TargetEpoch {
		http.Error(w, fmt.Sprintf("node %s is not the leader of %s for epoch %d", n.opts.ID, req.Target, req.TargetEpoch), http.StatusConflict)
		return
	}

	start := time.Now()
	res, err := n.pull(r.Context(), part, req)
	if err != nil {
		log.Printf("Node %s: migration into %s failed: %v", n.opts.ID, req.Target, err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	log.Printf("Node %s: migration into %s done: %d keys from %d source partitions in %s", n.opts.ID, req.Target, res.Keys, len(req.Sources), time.Since(start).Round(time.Millisecond))
	writeJSON(w, res)
}

func (n *Node) pull(ctx context.Context, part *partition.Partition, req model.MigrationRequest) (model.MigrationResult, error) {
	var res model.MigrationResult
	exportURL := func(src model.MigrationSource, freeze bool, after int64) string {
		f := 0
		if freeze {
			f = 1
		}
		return fmt.Sprintf("http://%s/migrate/export?gen=%d&pid=%d&target_count=%d&target_id=%d&freeze=%d&after=%d",
			src.Address, req.SourceGeneration, src.PartitionID, req.TargetCount, req.Target.ID, f, after)
	}
	fetch := func(src model.MigrationSource, freeze bool, after int64) (model.MigrationChunk, error) {
		var chunk model.MigrationChunk
		if err := n.longClient.Do(ctx, http.MethodGet, exportURL(src, freeze, after), nil, &chunk); err != nil {
			return chunk, fmt.Errorf("source partition %d: %w", src.PartitionID, err)
		}
		if chunk.Epoch != src.Epoch {
			return chunk, fmt.Errorf("source partition %d changed leader (epoch %d, expected %d)", src.PartitionID, chunk.Epoch, src.Epoch)
		}
		return chunk, nil
	}
	importOps := func(ops []model.Operation) error {
		for len(ops) > 0 {
			batch := ops
			if len(batch) > 1000 {
				batch = ops[:1000]
			}
			if err := part.Import(batch); err != nil {
				return err
			}
			res.Imported += len(batch)
			ops = ops[len(batch):]
		}
		return nil
	}
	setOps := func(data map[string]string) []model.Operation {
		ops := make([]model.Operation, 0, len(data))
		for k, v := range data {
			ops = append(ops, model.Operation{Type: model.OpSet, Key: k, Value: v})
		}
		return ops
	}

	// Anything left over from an earlier, interrupted attempt that no source has any more must go.
	existing, err := part.Snapshot(nil)
	if err != nil {
		return res, err
	}

	// Phase 1: bulk copy from snapshots; the old partitions keep taking writes meanwhile.
	seen := make(map[string]bool)
	perSource := make(map[int]map[string]bool)
	seqs := make(map[int]int64)
	for _, src := range req.Sources {
		chunk, err := fetch(src, false, 0)
		if err != nil {
			return res, err
		}
		keys := make(map[string]bool, len(chunk.Data))
		for k := range chunk.Data {
			keys[k] = true
			seen[k] = true
		}
		perSource[src.PartitionID] = keys
		seqs[src.PartitionID] = chunk.Seq
		if err := importOps(setOps(chunk.Data)); err != nil {
			return res, err
		}
	}
	var stale []model.Operation
	for k := range existing.Data {
		if !seen[k] {
			stale = append(stale, model.Operation{Type: model.OpDelete, Key: k})
		}
	}
	if err := importOps(stale); err != nil {
		return res, err
	}

	// Phase 2: freeze the keys on each old leader and replay what changed since its snapshot.
	for _, src := range req.Sources {
		chunk, err := fetch(src, true, seqs[src.PartitionID])
		if err != nil {
			return res, err
		}
		if chunk.Mode == "logs" {
			ops := make([]model.Operation, 0, len(chunk.Entries))
			for _, e := range chunk.Entries {
				ops = append(ops, e.Operation)
			}
			if err := importOps(ops); err != nil {
				return res, err
			}
			continue
		}
		// The old WAL no longer reaches back to our snapshot; take the full (frozen) set instead.
		ops := setOps(chunk.Data)
		for k := range perSource[src.PartitionID] {
			if _, ok := chunk.Data[k]; !ok {
				ops = append(ops, model.Operation{Type: model.OpDelete, Key: k})
			}
		}
		if err := importOps(ops); err != nil {
			return res, err
		}
	}

	res.Keys = part.Keys()
	return res, nil
}

func (n *Node) logStats(ctx context.Context) {
	var rates metrics.RateTracker
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		st := n.status()
		leaders, keys := 0, 0
		for _, p := range st.Partitions {
			if p.Role == model.NodeRoleLeader {
				leaders++
				keys += p.Keys
			}
		}
		r := rates.Update(st.Ops)
		log.Printf("Node %s: %d partitions (%d leading, %d keys led) | set %.0f/s get %.0f/s delete %.0f/s replicated %.0f/s | layout v%d",
			n.opts.ID, len(st.Partitions), leaders, keys, r["set"], r["get"], r["delete"], r["replicated"], st.LayoutVersion)
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
