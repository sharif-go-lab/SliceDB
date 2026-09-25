package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
	"github.com/sharif-go-lab/SliceDB/pkg/network"
)

const maxEvents = 300

type Options struct {
	ID                string
	Listen            string
	Advertise         string
	Etcd              []string
	PartitionCount    int // initial config, used only if etcd has none yet
	ReplicationFactor int
	Interval          time.Duration
	AutoBalance       bool
	SessionTTL        int
}

type Event struct {
	Time    time.Time `json:"time"`
	Message string    `json:"message"`
}

// fence identifies the leadership term: the election key and its create revision. Every layout
// write is conditioned on it, so a controller that lost leadership cannot write any more.
type fence struct {
	key string
	rev int64
}

// Controller can run in any number of copies. They all campaign in an etcd election; the winner
// runs the control loop, the others serve the panel and forward changes to the winner. All state
// lives in etcd, so a new leader simply continues where the previous one stopped.
type Controller struct {
	opts       Options
	registry   *registry.EtcdRegistry
	client     *network.Client
	longClient *network.Client
	nodes      *registry.NodeWatcher
	layout     *registry.LayoutWatcher

	leading atomic.Bool
	fence   atomic.Pointer[fence]
	trigger chan struct{}

	mu       sync.Mutex
	statuses map[string]model.NodeStatus
	events   []Event
}

func NewController(opts Options) (*Controller, error) {
	reg, err := registry.NewEtcdRegistry(opts.Etcd)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Second
	}
	if opts.SessionTTL <= 0 {
		opts.SessionTTL = 5
	}
	return &Controller{
		opts:       opts,
		registry:   reg,
		client:     network.NewClientWithTimeout(2 * time.Second),
		longClient: network.NewClientWithTimeout(2 * time.Minute),
		trigger:    make(chan struct{}, 1),
		statuses:   make(map[string]model.NodeStatus),
	}, nil
}

// Start serves until ctx is cancelled (SIGTERM); a leader then resigns so a standby controller
// takes over at once instead of after the session TTL.
func (c *Controller) Start(ctx context.Context) error {
	c.nodes = c.registry.WatchNodes(ctx, c.kick)
	c.layout = c.registry.WatchLayout(ctx, nil)
	c.registry.WatchPrefix(ctx, registry.ConfigKey, c.kick)
	c.registry.WatchPrefix(ctx, registry.DrainPrefix, c.kick)
	go c.announce(ctx)
	campaignDone := make(chan struct{})
	go func() {
		defer close(campaignDone)
		c.campaign(ctx)
	}()

	mux := http.NewServeMux()
	c.routes(mux)
	server := &http.Server{Addr: c.opts.Listen, Handler: mux}
	go func() {
		<-ctx.Done()
		<-campaignDone
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("Controller %s starting on %s (advertised as %s)", c.opts.ID, c.opts.Listen, c.opts.Advertise)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// kick asks the leader loop to run a round now (membership or config changed).
func (c *Controller) kick() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

func (c *Controller) record(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("Controller %s: %s", c.opts.ID, msg)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, Event{Time: time.Now(), Message: msg})
	if len(c.events) > maxEvents {
		c.events = c.events[len(c.events)-maxEvents:]
	}
}

// announce keeps this controller listed under /controllers/ (for the panel) with a lease.
func (c *Controller) announce(ctx context.Context) {
	info := registry.ControllerInfo{ID: c.opts.ID, Address: c.opts.Advertise}
	for ctx.Err() == nil {
		leaseID, lost, err := c.registry.RegisterController(ctx, info, int64(c.opts.SessionTTL))
		if err != nil {
			log.Printf("Controller %s: registering in etcd failed: %v", c.opts.ID, err)
			time.Sleep(time.Second)
			continue
		}
		<-lost
		if ctx.Err() != nil {
			c.registry.Revoke(leaseID)
		}
	}
}

// campaign runs the etcd election forever. Winning it starts the control loop; losing the
// session (lease expired, e.g. the controller was cut off from etcd) stops it immediately.
func (c *Controller) campaign(ctx context.Context) {
	for ctx.Err() == nil {
		session, err := concurrency.NewSession(c.registry.Client(), concurrency.WithTTL(c.opts.SessionTTL))
		if err != nil {
			log.Printf("Controller %s: creating etcd session failed: %v", c.opts.ID, err)
			time.Sleep(time.Second)
			continue
		}
		election := concurrency.NewElection(session, registry.ElectionPrefix)
		termCtx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-session.Done():
				cancel()
			case <-termCtx.Done():
			}
		}()

		log.Printf("Controller %s: campaigning for leadership", c.opts.ID)
		if err := election.Campaign(termCtx, c.opts.Advertise); err != nil {
			log.Printf("Controller %s: campaign ended: %v", c.opts.ID, err)
			cancel()
			_ = session.Close()
			time.Sleep(time.Second)
			continue
		}

		c.fence.Store(&fence{key: election.Key(), rev: election.Rev()})
		c.leading.Store(true)
		c.record("controller %s is now the leader (election rev %d)", c.opts.ID, election.Rev())
		c.lead(termCtx)
		c.leading.Store(false)
		c.fence.Store(nil)
		c.record("controller %s stopped leading", c.opts.ID)

		cancel()
		resignCtx, resignCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = election.Resign(resignCtx)
		resignCancel()
		_ = session.Close()
	}
}

func (c *Controller) lead(ctx context.Context) {
	cfg := model.Config{PartitionCount: c.opts.PartitionCount, ReplicationFactor: c.opts.ReplicationFactor}
	if err := c.registry.InitConfig(ctx, cfg); err != nil {
		log.Printf("Controller %s: initializing config failed: %v", c.opts.ID, err)
	}

	go c.reshardLoop(ctx)

	ticker := time.NewTicker(c.opts.Interval)
	defer ticker.Stop()
	for {
		c.round(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.trigger:
		}
	}
}

// round is one pass of the control loop: read the desired config and the live membership from
// etcd, ask every node for its status, plan, and write the new layout back (fenced CAS).
func (c *Controller) round(ctx context.Context) {
	cfg, err := c.registry.GetConfig(ctx)
	if err != nil {
		log.Printf("Controller %s: reading config failed: %v", c.opts.ID, err)
		return
	}
	drained, err := c.registry.GetDrained(ctx)
	if err != nil {
		log.Printf("Controller %s: reading drained nodes failed: %v", c.opts.ID, err)
		return
	}
	nodes := c.nodes.Snapshot()
	statuses := c.pollStatuses(ctx, nodes)
	c.mu.Lock()
	c.statuses = statuses
	c.mu.Unlock()

	events, err := c.updateLayout(ctx, func(l *model.Layout) ([]string, error) {
		return plan(l, planInput{
			Config:      cfg,
			Nodes:       nodes,
			Drained:     drained,
			Status:      statuses,
			Now:         time.Now(),
			AutoBalance: c.opts.AutoBalance,
		}), nil
	})
	if err != nil && ctx.Err() == nil {
		log.Printf("Controller %s: updating layout failed: %v", c.opts.ID, err)
	}
	for _, e := range events {
		c.record("%s", e)
	}
}

func (c *Controller) pollStatuses(ctx context.Context, nodes map[string]model.Node) map[string]model.NodeStatus {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out = make(map[string]model.NodeStatus)
	)
	for id, node := range nodes {
		wg.Add(1)
		go func(id string, node model.Node) {
			defer wg.Done()
			reqCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			var st model.NodeStatus
			if err := c.client.Do(reqCtx, http.MethodGet, "http://"+node.Address+"/status", nil, &st); err != nil {
				return
			}
			if st.Incarnation != node.Incarnation {
				return
			}
			mu.Lock()
			out[id] = st
			mu.Unlock()
		}(id, node)
	}
	wg.Wait()
	return out
}

var errNotLeader = errors.New("this controller is not the leader")

// updateLayout applies mutate to the latest layout and stores it with a compare-and-swap that
// is also fenced on our election key, retrying on concurrent changes.
func (c *Controller) updateLayout(ctx context.Context, mutate func(l *model.Layout) ([]string, error)) ([]string, error) {
	for attempt := 0; attempt < 5; attempt++ {
		f := c.fence.Load()
		if f == nil {
			return nil, errNotLeader
		}
		current, rev, err := c.registry.GetLayout(ctx)
		if err != nil {
			return nil, err
		}
		next := &model.Layout{Members: make(map[string]int64)}
		if current != nil {
			next = current.Clone()
		}
		next.Version++

		events, err := mutate(next)
		if err != nil {
			return nil, err
		}
		if current != nil && sameLayout(current, next) {
			return events, nil
		}
		if err := c.registry.PutLayout(ctx, next, rev, f.key, f.rev); err != nil {
			if errors.Is(err, registry.ErrConflict) {
				continue
			}
			return nil, err
		}
		return events, nil
	}
	return nil, registry.ErrConflict
}

func sameLayout(a, b *model.Layout) bool {
	version := b.Version
	b.Version = a.Version
	defer func() { b.Version = version }()
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// reshardLoop drives the migration of §13 while the leader holds office: for every partition
// of the next generation, its leader pulls the matching keys from the old partitions, and once
// that succeeded the partition is marked done so the load balancer routes its keys to it.
// When all are done, the next generation becomes the current one.
func (c *Controller) reshardLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	lastErr := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := c.reshardStep(ctx); err != nil && ctx.Err() == nil {
			if err.Error() != lastErr {
				c.record("resharding: %v", err)
			}
			lastErr = err.Error()
		} else {
			lastErr = ""
		}
	}
}

func (c *Controller) reshardStep(ctx context.Context) error {
	l, _, err := c.registry.GetLayout(ctx)
	if err != nil || l == nil || l.Resharding == nil {
		return err
	}
	r := l.Resharding

	allDone := true
	for _, s := range r.Status {
		allDone = allDone && s == model.MigrationDone
	}
	if allDone {
		events, err := c.updateLayout(ctx, func(n *model.Layout) ([]string, error) {
			if n.Resharding == nil || n.Resharding.Generation != r.Generation {
				return nil, nil
			}
			for _, s := range n.Resharding.Status {
				if s != model.MigrationDone {
					return nil, nil
				}
			}
			from := n.PartitionCount
			next := n.Resharding
			n.Generation, n.PartitionCount, n.Partitions, n.Resharding = next.Generation, next.PartitionCount, next.Partitions, nil
			return []string{fmt.Sprintf("resharding finished: %d -> %d partitions, generation %d is now current; old partitions are dropped", from, n.PartitionCount, n.Generation)}, nil
		})
		for _, e := range events {
			c.record("%s", e)
		}
		return err
	}

	c.mu.Lock()
	statuses := c.statuses
	c.mu.Unlock()

	for i := range r.Partitions {
		if r.Status[i] == model.MigrationDone {
			continue
		}
		target := r.Partitions[i]
		ref := model.PartitionRef{Generation: r.Generation, ID: i}
		if !target.Writable() {
			continue
		}
		leaderNode, ok := c.nodes.Get(target.Leader)
		if !ok {
			continue
		}
		st, ok := statuses[target.Leader]
		if !ok {
			continue
		}
		if ps, ok := st.Partition(ref); !ok || ps.Role != model.NodeRoleLeader || ps.Epoch != target.Epoch {
			continue // the node has not picked up its new partition yet
		}

		req := model.MigrationRequest{Target: ref, TargetEpoch: target.Epoch, TargetCount: r.PartitionCount, SourceGeneration: l.Generation}
		ready := true
		for _, sid := range hash.SourcePartitions(i, l.PartitionCount, r.PartitionCount) {
			src := l.Partitions[sid]
			srcNode, ok := c.nodes.Get(src.Leader)
			if !src.Writable() || !ok {
				ready = false
				break
			}
			req.Sources = append(req.Sources, model.MigrationSource{PartitionID: sid, Epoch: src.Epoch, Address: srcNode.Address})
		}
		if !ready {
			continue
		}

		start := time.Now()
		var res model.MigrationResult
		if err := c.longClient.Do(ctx, http.MethodPost, "http://"+leaderNode.Address+"/migrate/pull", req, &res); err != nil {
			return fmt.Errorf("migrating into %s on %s failed (will retry): %w", ref, target.Leader, err)
		}

		events, err := c.updateLayout(ctx, func(n *model.Layout) ([]string, error) {
			nr := n.Resharding
			if nr == nil || nr.Generation != r.Generation {
				return nil, errors.New("resharding was replaced meanwhile")
			}
			if np := nr.Partitions[i]; np.Leader != target.Leader || np.Epoch != target.Epoch {
				return nil, fmt.Errorf("%s changed leader during its migration; retrying", ref)
			}
			for _, src := range req.Sources {
				if sp := n.Partitions[src.PartitionID]; sp.Epoch != src.Epoch {
					return nil, fmt.Errorf("source partition %d changed leader during the migration of %s; retrying", src.PartitionID, ref)
				}
			}
			nr.Status[i] = model.MigrationDone
			return []string{fmt.Sprintf("resharding: %s is live on %s (%d keys from %d old partitions in %s)",
				ref, target.Leader, res.Keys, len(req.Sources), time.Since(start).Round(time.Millisecond))}, nil
		})
		for _, e := range events {
			c.record("%s", e)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
