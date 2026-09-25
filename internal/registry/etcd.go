package registry

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

// Everything that used to live only in the controller's memory is kept in etcd under these keys.
const (
	NodesPrefix       = "/slicedb/nodes/"       // lease-bound membership of database nodes
	ControllersPrefix = "/slicedb/controllers/" // lease-bound list of running controllers
	ElectionPrefix    = "/slicedb/controller-election"
	ConfigKey         = "/slicedb/config" // desired partition count / replication factor
	LayoutKey         = "/slicedb/layout" // routing table, written only by the elected controller
	DrainPrefix       = "/slicedb/drain/" // nodes an operator asked to empty
)

// ErrConflict means the layout changed since it was read, or the writer is no longer the leader.
var ErrConflict = errors.New("layout changed concurrently or controller leadership was lost")

type ControllerInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type EtcdRegistry struct {
	client *clientv3.Client
}

func NewEtcdRegistry(endpoints []string) (*EtcdRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    5 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdRegistry{client: cli}, nil
}

func (r *EtcdRegistry) Client() *clientv3.Client {
	return r.client
}

// Register stores value under key, bound to a lease that is kept alive in the background.
// The returned channel is closed once the lease is lost (it expired because etcd could not be
// reached for longer than ttl, or ctx ended); by then etcd has already deleted the key.
func (r *EtcdRegistry) Register(ctx context.Context, key string, value interface{}, ttl int64) (clientv3.LeaseID, <-chan struct{}, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, nil, err
	}

	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lease, err := r.client.Grant(opCtx, ttl)
	if err != nil {
		return 0, nil, err
	}
	if _, err := r.client.Put(opCtx, key, string(data), clientv3.WithLease(lease.ID)); err != nil {
		_, _ = r.client.Revoke(context.Background(), lease.ID)
		return 0, nil, err
	}

	ch, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return 0, nil, err
	}
	lost := make(chan struct{})
	go func() {
		for range ch {
			// consume keep alive responses
		}
		close(lost)
	}()
	return lease.ID, lost, nil
}

// Revoke ends a lease right away, deleting its keys: a graceful exit is noticed immediately
// instead of after the TTL.
func (r *EtcdRegistry) Revoke(leaseID clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = r.client.Revoke(ctx, leaseID)
}

func (r *EtcdRegistry) RegisterNode(ctx context.Context, node model.Node, ttl int64) (clientv3.LeaseID, <-chan struct{}, error) {
	return r.Register(ctx, NodesPrefix+node.ID, node, ttl)
}

func (r *EtcdRegistry) RegisterController(ctx context.Context, info ControllerInfo, ttl int64) (clientv3.LeaseID, <-chan struct{}, error) {
	return r.Register(ctx, ControllersPrefix+info.ID, info, ttl)
}

func (r *EtcdRegistry) GetNodes(ctx context.Context) (map[string]model.Node, error) {
	resp, err := r.client.Get(ctx, NodesPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]model.Node)
	for _, kv := range resp.Kvs {
		var n model.Node
		if err := json.Unmarshal(kv.Value, &n); err == nil {
			nodes[n.ID] = n
		}
	}
	return nodes, nil
}

func (r *EtcdRegistry) GetControllers(ctx context.Context) ([]ControllerInfo, error) {
	resp, err := r.client.Get(ctx, ControllersPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var controllers []ControllerInfo
	for _, kv := range resp.Kvs {
		var c ControllerInfo
		if err := json.Unmarshal(kv.Value, &c); err == nil {
			controllers = append(controllers, c)
		}
	}
	sort.Slice(controllers, func(i, j int) bool { return controllers[i].ID < controllers[j].ID })
	return controllers, nil
}

// ElectionLeader returns the value (HTTP address) proposed by the current controller leader.
func (r *EtcdRegistry) ElectionLeader(ctx context.Context) (string, error) {
	resp, err := r.client.Get(ctx, ElectionPrefix+"/", clientv3.WithFirstCreate()...)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// InitConfig stores cfg unless a config already exists (the first controller wins).
func (r *EtcdRegistry) InitConfig(ctx context.Context, cfg model.Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(ConfigKey), "=", 0)).
		Then(clientv3.OpPut(ConfigKey, string(data))).
		Commit()
	return err
}

func (r *EtcdRegistry) GetConfig(ctx context.Context) (model.Config, error) {
	var cfg model.Config
	resp, err := r.client.Get(ctx, ConfigKey)
	if err != nil {
		return cfg, err
	}
	if len(resp.Kvs) == 0 {
		return cfg, errors.New("cluster config not initialized yet")
	}
	err = json.Unmarshal(resp.Kvs[0].Value, &cfg)
	return cfg, err
}

func (r *EtcdRegistry) PutConfig(ctx context.Context, cfg model.Config) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = r.client.Put(ctx, ConfigKey, string(data))
	return err
}

func (r *EtcdRegistry) SetDrained(ctx context.Context, nodeID string, drained bool) error {
	var err error
	if drained {
		_, err = r.client.Put(ctx, DrainPrefix+nodeID, time.Now().UTC().Format(time.RFC3339))
	} else {
		_, err = r.client.Delete(ctx, DrainPrefix+nodeID)
	}
	return err
}

func (r *EtcdRegistry) GetDrained(ctx context.Context) (map[string]bool, error) {
	resp, err := r.client.Get(ctx, DrainPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	drained := make(map[string]bool)
	for _, kv := range resp.Kvs {
		drained[strings.TrimPrefix(string(kv.Key), DrainPrefix)] = true
	}
	return drained, nil
}

// GetLayout returns the stored layout (nil if none yet) and its mod revision for PutLayout.
func (r *EtcdRegistry) GetLayout(ctx context.Context) (*model.Layout, int64, error) {
	resp, err := r.client.Get(ctx, LayoutKey)
	if err != nil {
		return nil, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, nil
	}
	var l model.Layout
	if err := json.Unmarshal(resp.Kvs[0].Value, &l); err != nil {
		return nil, 0, err
	}
	if l.Members == nil {
		l.Members = make(map[string]int64)
	}
	return &l, resp.Kvs[0].ModRevision, nil
}

// PutLayout writes l only if the stored layout is still at prevModRev (0 = absent) and, when
// fenceKey is given, only while that election key still exists with fenceRev — that is, while
// the caller is still the elected controller. A deposed leader can therefore never overwrite
// the decisions of its successor.
func (r *EtcdRegistry) PutLayout(ctx context.Context, l *model.Layout, prevModRev int64, fenceKey string, fenceRev int64) error {
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	cmps := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(LayoutKey), "=", prevModRev)}
	if fenceKey != "" {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(fenceKey), "=", fenceRev))
	}
	resp, err := r.client.Txn(ctx).If(cmps...).Then(clientv3.OpPut(LayoutKey, string(data))).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrConflict
	}
	return nil
}

// NodeWatcher mirrors the live node membership through an etcd watch.
type NodeWatcher struct {
	mu    sync.RWMutex
	nodes map[string]model.Node
}

func (w *NodeWatcher) Get(id string) (model.Node, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	n, ok := w.nodes[id]
	return n, ok
}

func (w *NodeWatcher) Snapshot() map[string]model.Node {
	w.mu.RLock()
	defer w.mu.RUnlock()

	out := make(map[string]model.Node, len(w.nodes))
	for k, v := range w.nodes {
		out[k] = v
	}
	return out
}

func (r *EtcdRegistry) WatchNodes(ctx context.Context, onChange func()) *NodeWatcher {
	w := &NodeWatcher{nodes: make(map[string]model.Node)}
	reset := func(resp *clientv3.GetResponse) {
		nodes := make(map[string]model.Node)
		for _, kv := range resp.Kvs {
			var n model.Node
			if err := json.Unmarshal(kv.Value, &n); err == nil {
				nodes[n.ID] = n
			}
		}
		w.mu.Lock()
		w.nodes = nodes
		w.mu.Unlock()
	}
	apply := func(ev *clientv3.Event) {
		id := strings.TrimPrefix(string(ev.Kv.Key), NodesPrefix)
		w.mu.Lock()
		defer w.mu.Unlock()
		if ev.Type == clientv3.EventTypeDelete {
			delete(w.nodes, id)
			return
		}
		var n model.Node
		if err := json.Unmarshal(ev.Kv.Value, &n); err == nil {
			w.nodes[n.ID] = n
		}
	}
	go r.watch(ctx, NodesPrefix, true, reset, apply, onChange)
	return w
}

// LayoutWatcher keeps the latest routing table, pushed by etcd as soon as the controller writes it.
type LayoutWatcher struct {
	v atomic.Pointer[model.Layout]
}

// Load returns the current layout (nil until one exists). Callers must not modify it.
func (w *LayoutWatcher) Load() *model.Layout {
	return w.v.Load()
}

func (r *EtcdRegistry) WatchLayout(ctx context.Context, onChange func(*model.Layout)) *LayoutWatcher {
	w := &LayoutWatcher{}
	store := func(data []byte) {
		var l model.Layout
		if err := json.Unmarshal(data, &l); err != nil {
			log.Printf("etcd: ignoring malformed layout: %v", err)
			return
		}
		if l.Members == nil {
			l.Members = make(map[string]int64)
		}
		w.v.Store(&l)
	}
	reset := func(resp *clientv3.GetResponse) {
		if len(resp.Kvs) == 0 {
			w.v.Store(nil)
			return
		}
		store(resp.Kvs[0].Value)
	}
	apply := func(ev *clientv3.Event) {
		if ev.Type == clientv3.EventTypeDelete {
			w.v.Store(nil)
			return
		}
		store(ev.Kv.Value)
	}
	changed := func() {
		if onChange != nil {
			onChange(w.Load())
		}
	}
	go r.watch(ctx, LayoutKey, false, reset, apply, changed)
	return w
}

// WatchPrefix calls onChange whenever something under prefix changes.
func (r *EtcdRegistry) WatchPrefix(ctx context.Context, prefix string, onChange func()) {
	go r.watch(ctx, prefix, true, func(*clientv3.GetResponse) {}, func(*clientv3.Event) {}, onChange)
}

// watch loads key (or prefix) and then follows it with an etcd watch starting right after the
// revision that was read, so no update is missed. Whenever the watch breaks (etcd member down,
// history compacted, ...) it starts over with a fresh read.
func (r *EtcdRegistry) watch(ctx context.Context, key string, prefix bool, reset func(*clientv3.GetResponse), apply func(*clientv3.Event), onChange func()) {
	var opts []clientv3.OpOption
	if prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	notify := func() {
		if onChange != nil {
			onChange()
		}
	}

	for ctx.Err() == nil {
		getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := r.client.Get(getCtx, key, opts...)
		cancel()
		if err != nil {
			log.Printf("etcd: reading %s failed: %v", key, err)
			sleep(ctx, time.Second)
			continue
		}
		reset(resp)
		notify()

		watchOpts := append(append([]clientv3.OpOption{}, opts...), clientv3.WithRev(resp.Header.Revision+1))
		watchCtx, cancelWatch := context.WithCancel(clientv3.WithRequireLeader(ctx))
		for wresp := range r.client.Watch(watchCtx, key, watchOpts...) {
			if err := wresp.Err(); err != nil {
				log.Printf("etcd: watch on %s interrupted: %v", key, err)
				break
			}
			for _, ev := range wresp.Events {
				apply(ev)
			}
			if len(wresp.Events) > 0 {
				notify()
			}
		}
		cancelWatch()
		sleep(ctx, 200*time.Millisecond)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
