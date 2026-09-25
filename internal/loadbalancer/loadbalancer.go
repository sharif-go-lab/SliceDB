package loadbalancer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/metrics"
	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
)

const (
	StrategyLeader     = "leader"      // reads go to the leader; followers only if it fails
	StrategyRoundRobin = "round-robin" // reads rotate over the leader and in-sync followers
	StrategyFollowers  = "followers"   // reads prefer followers, keeping load off the leader
)

type Options struct {
	Listen       string
	Etcd         []string
	ReadStrategy string
	MaxAttempts  int
	Timeout      time.Duration
}

// LoadBalancer is stateless: it routes by the layout it watches in etcd, so any number of
// copies can run side by side.
type LoadBalancer struct {
	opts     Options
	registry *registry.EtcdRegistry
	nodes    *registry.NodeWatcher
	layout   *registry.LayoutWatcher
	http     *http.Client
	rr       atomic.Uint64
	ops      *metrics.Counters
	rates    metrics.RateTracker
}

func New(opts Options) (*LoadBalancer, error) {
	reg, err := registry.NewEtcdRegistry(opts.Etcd)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 8
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 3 * time.Second
	}
	switch opts.ReadStrategy {
	case StrategyLeader, StrategyRoundRobin, StrategyFollowers:
	default:
		return nil, fmt.Errorf("unknown read strategy %q", opts.ReadStrategy)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 2048
	transport.MaxIdleConnsPerHost = 512
	return &LoadBalancer{
		opts:     opts,
		registry: reg,
		http:     &http.Client{Timeout: opts.Timeout, Transport: transport},
		ops:      metrics.NewCounters(),
	}, nil
}

func (lb *LoadBalancer) Start() error {
	ctx := context.Background()
	lb.nodes = lb.registry.WatchNodes(ctx, nil)
	lb.layout = lb.registry.WatchLayout(ctx, func(l *model.Layout) {
		if l == nil {
			return
		}
		resharding := ""
		if l.Resharding != nil {
			resharding = fmt.Sprintf(", resharding to %d", l.Resharding.PartitionCount)
		}
		log.Printf("LB: routing table updated to v%d (generation %d, %d partitions%s)", l.Version, l.Generation, l.PartitionCount, resharding)
	})
	go lb.logStats(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/set", lb.handleSet)
	mux.HandleFunc("/get", lb.handleGet)
	mux.HandleFunc("/delete", lb.handleDelete)
	mux.HandleFunc("/health", lb.handleHealth)
	mux.HandleFunc("/metrics", lb.handleMetrics)
	mux.HandleFunc("/layout", lb.handleLayout)

	log.Printf("LB starting on %s (read strategy %s)", lb.opts.Listen, lb.opts.ReadStrategy)
	return http.ListenAndServe(lb.opts.Listen, mux)
}

func (lb *LoadBalancer) handleSet(w http.ResponseWriter, r *http.Request) {
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
	payload, _ := json.Marshal(data)
	lb.write(w, r, "set", http.MethodPost, data.Key, payload)
}

func (lb *LoadBalancer) handleDelete(w http.ResponseWriter, r *http.Request) {
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
	payload, _ := json.Marshal(data)
	lb.write(w, r, "delete", http.MethodDelete, data.Key, payload)
}

// write sends a Set/Delete to the leader of the key's partition. If there is no leader right now
// (failover), the leadership is being handed off, or the node rejects the request because our
// routing table is stale, it waits a little for the new layout and tries again.
func (lb *LoadBalancer) write(w http.ResponseWriter, r *http.Request, op, method, key string, payload []byte) {
	lastErr := "no attempt made"
	for attempt := 0; attempt < lb.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			lb.ops.Inc(op + ".retry")
			if !pause(r.Context(), backoff(attempt)) {
				return
			}
		}
		ref, p := lb.layout.Load().Route(key)
		if p == nil {
			lastErr = "no routing table yet"
			continue
		}
		if !p.Writable() {
			lastErr = fmt.Sprintf("partition %s has no writable leader right now", ref)
			continue
		}
		code, body, err := lb.forward(r.Context(), method, "/"+op, p.Leader, ref, key, payload)
		if err != nil {
			lastErr = err.Error()
			continue
		}
		if retryable(code) {
			lastErr = fmt.Sprintf("%s answered %d: %s", p.Leader, code, bytes.TrimSpace(body))
			continue
		}
		lb.finish(w, op, code, body)
		return
	}
	lb.ops.Inc(op + ".failed")
	http.Error(w, "unavailable: "+lastErr, http.StatusServiceUnavailable)
}

// handleGet reads from the leader or an in-sync follower depending on the strategy, falling
// back to the other replicas when one does not answer — reads keep working during a failover.
func (lb *LoadBalancer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter required", http.StatusBadRequest)
		return
	}

	lastErr := "no attempt made"
	for attempt := 0; attempt < lb.opts.MaxAttempts; attempt++ {
		if attempt > 0 {
			lb.ops.Inc("get.retry")
			if !pause(r.Context(), backoff(attempt)) {
				return
			}
		}
		ref, p := lb.layout.Load().Route(key)
		if p == nil {
			lastErr = "no routing table yet"
			continue
		}
		candidates := lb.readCandidates(p)
		if len(candidates) == 0 {
			lastErr = fmt.Sprintf("partition %s has no readable replica", ref)
			continue
		}
		for _, nodeID := range candidates {
			code, body, err := lb.forward(r.Context(), http.MethodGet, "/get", nodeID, ref, key, nil)
			if err != nil {
				lastErr = err.Error()
				continue
			}
			if retryable(code) {
				lastErr = fmt.Sprintf("%s answered %d: %s", nodeID, code, bytes.TrimSpace(body))
				continue
			}
			lb.finish(w, "get", code, body)
			return
		}
	}
	lb.ops.Inc("get.failed")
	http.Error(w, "unavailable: "+lastErr, http.StatusServiceUnavailable)
}

func (lb *LoadBalancer) readCandidates(p *model.Partition) []string {
	replicas := p.ReadReplicas()
	if len(replicas) <= 1 || lb.opts.ReadStrategy == StrategyLeader || p.Leader == "" {
		return replicas
	}
	if lb.opts.ReadStrategy == StrategyFollowers {
		followers := rotate(p.Followers, lb.rr.Add(1))
		return append(followers, p.Leader)
	}
	return rotate(replicas, lb.rr.Add(1))
}

func rotate(list []string, n uint64) []string {
	out := make([]string, 0, len(list))
	start := int(n % uint64(len(list)))
	out = append(out, list[start:]...)
	return append(out, list[:start]...)
}

func (lb *LoadBalancer) forward(ctx context.Context, method, path, nodeID string, ref model.PartitionRef, key string, payload []byte) (int, []byte, error) {
	node, ok := lb.nodes.Get(nodeID)
	if !ok {
		return 0, nil, fmt.Errorf("node %s is not registered", nodeID)
	}
	target := fmt.Sprintf("http://%s%s?gen=%d&pid=%d", node.Address, path, ref.Generation, ref.ID)
	var body io.Reader
	if payload == nil {
		target += "&key=" + url.QueryEscape(key)
	} else {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	lb.ops.Inc("node." + nodeID)
	resp, err := lb.http.Do(req)
	if err != nil {
		lb.ops.Inc("node." + nodeID + ".error")
		return 0, nil, fmt.Errorf("%s: %w", nodeID, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

// retryable reports whether another attempt (possibly on another node / with a newer layout)
// might succeed: the node is not the right one any more, is syncing, or failed.
func retryable(code int) bool {
	return code == http.StatusMisdirectedRequest || code == http.StatusConflict || code >= 500
}

func (lb *LoadBalancer) finish(w http.ResponseWriter, op string, code int, body []byte) {
	switch {
	case code == http.StatusOK:
		lb.ops.Inc(op + ".ok")
	case code == http.StatusNotFound:
		lb.ops.Inc(op + ".notfound")
	default:
		lb.ops.Inc(op + ".error")
	}
	if code == http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(code)
	_, _ = w.Write(body)
}

func (lb *LoadBalancer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	status := map[string]interface{}{"status": "healthy"}
	if l := lb.layout.Load(); l != nil {
		status["layout_version"] = l.Version
	}
	writeJSON(w, status)
}

func (lb *LoadBalancer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]interface{}{
		"counters":   lb.ops.Snapshot(),
		"per_second": lb.rates.Rates(),
	})
}

func (lb *LoadBalancer) handleLayout(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, lb.layout.Load())
}

func (lb *LoadBalancer) logStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		r := lb.rates.Update(lb.ops.Snapshot())
		sum := func(op string) float64 {
			return r[op+".ok"] + r[op+".notfound"] + r[op+".error"] + r[op+".failed"]
		}
		total := sum("set") + sum("get") + sum("delete")
		if total == 0 {
			continue
		}
		log.Printf("LB: %.0f req/s (set %.0f, get %.0f, delete %.0f) | retries %.1f/s | failed %.1f/s",
			total, sum("set"), sum("get"), sum("delete"),
			r["set.retry"]+r["get.retry"]+r["delete.retry"],
			r["set.failed"]+r["get.failed"]+r["delete.failed"])
	}
}

func backoff(attempt int) time.Duration {
	d := 25 * time.Millisecond << uint(attempt)
	if d > 800*time.Millisecond {
		d = 800 * time.Millisecond
	}
	return d
}

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
