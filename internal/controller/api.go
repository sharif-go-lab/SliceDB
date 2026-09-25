package controller

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
)

//go:embed panel.html
var panelHTML []byte

const proxiedHeader = "X-SliceDB-Proxied-By"

func (c *Controller) routes(mux *http.ServeMux) {
	mux.HandleFunc("/", c.handlePanel)
	mux.HandleFunc("/health", c.handleHealth)
	mux.HandleFunc("/api/", c.handleAPI)
}

func (c *Controller) handlePanel(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(panelHTML)
}

func (c *Controller) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": "healthy", "id": c.opts.ID, "leading": c.leading.Load()})
}

// handleAPI serves the admin API. Only the elected controller changes the layout, so followers
// forward every API call to it; if no leader is reachable they still answer read-only state
// straight from etcd.
func (c *Controller) handleAPI(w http.ResponseWriter, r *http.Request) {
	if !c.leading.Load() {
		if r.Header.Get(proxiedHeader) == "" && c.proxyToLeader(w, r) {
			return
		}
		if r.Method == http.MethodGet && (r.URL.Path == "/api/state" || r.URL.Path == "/api/stable") {
			c.serveAPI(w, r)
			return
		}
		writeError(w, http.StatusServiceUnavailable, "no controller leader is available right now")
		return
	}
	c.serveAPI(w, r)
}

func (c *Controller) serveAPI(w http.ResponseWriter, r *http.Request) {
	routes := map[string]struct {
		method  string
		handler http.HandlerFunc
	}{
		"/api/state":             {http.MethodGet, c.handleState},
		"/api/stable":            {http.MethodGet, c.handleStable},
		"/api/config":            {http.MethodPost, c.handleConfig},
		"/api/nodes/drain":       {http.MethodPost, c.handleDrain},
		"/api/partitions/leader": {http.MethodPost, c.handleTransferLeader},
		"/api/partitions/move":   {http.MethodPost, c.handleMove},
		"/api/partitions/unpin":  {http.MethodPost, c.handleUnpin},
	}
	route, ok := routes[r.URL.Path]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown endpoint")
		return
	}
	if r.Method != route.method {
		writeError(w, http.StatusMethodNotAllowed, "use "+route.method)
		return
	}
	route.handler(w, r)
}

func (c *Controller) proxyToLeader(w http.ResponseWriter, r *http.Request) bool {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	addr, err := c.registry.ElectionLeader(ctx)
	cancel()
	if err != nil || addr == "" || addr == c.opts.Advertise {
		return false
	}
	target, err := url.Parse("http://" + addr)
	if err != nil {
		return false
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if r.Method == http.MethodGet {
			c.serveAPI(w, r)
			return
		}
		writeError(w, http.StatusBadGateway, fmt.Sprintf("leader %s unreachable: %v", addr, err))
	}
	r.Header.Set(proxiedHeader, c.opts.ID)
	proxy.ServeHTTP(w, r)
	return true
}

type stateResponse struct {
	Controller  string                      `json:"controller"`
	Leading     bool                        `json:"leading"`
	Leader      string                      `json:"leader"`
	Controllers []registry.ControllerInfo   `json:"controllers"`
	Config      model.Config                `json:"config"`
	Layout      *model.Layout               `json:"layout"`
	Nodes       []model.Node                `json:"nodes"`
	Drained     map[string]bool             `json:"drained"`
	Status      map[string]model.NodeStatus `json:"status"`
	Stable      string                      `json:"unstable_reason"`
	Events      []Event                     `json:"events"`
}

func (c *Controller) handleState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	st := stateResponse{Controller: c.opts.ID, Leading: c.leading.Load()}
	st.Leader, _ = c.registry.ElectionLeader(ctx)
	st.Controllers, _ = c.registry.GetControllers(ctx)
	st.Config, _ = c.registry.GetConfig(ctx)
	st.Layout, _, _ = c.registry.GetLayout(ctx)
	st.Drained, _ = c.registry.GetDrained(ctx)
	for _, n := range c.nodes.Snapshot() {
		st.Nodes = append(st.Nodes, n)
	}
	sort.Slice(st.Nodes, func(i, j int) bool { return st.Nodes[i].ID < st.Nodes[j].ID })
	st.Stable = unstableReason(st.Layout, st.Config, st.Drained, c.opts.AutoBalance)

	c.mu.Lock()
	st.Status = c.statuses
	st.Events = append([]Event(nil), c.events...)
	c.mu.Unlock()

	writeJSON(w, http.StatusOK, st)
}

// handleStable is a small endpoint for scripts: 200 once the cluster converged, 503 before.
func (c *Controller) handleStable(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	cfg, err := c.registry.GetConfig(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	l, _, err := c.registry.GetLayout(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	drained, _ := c.registry.GetDrained(ctx)
	if reason := unstableReason(l, cfg, drained, c.opts.AutoBalance); reason != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]interface{}{"stable": false, "reason": reason})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"stable": true, "version": l.Version})
}

func (c *Controller) handleConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PartitionCount    *int `json:"partition_count"`
		ReplicationFactor *int `json:"replication_factor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg, err := c.registry.GetConfig(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if req.PartitionCount != nil {
		if *req.PartitionCount < 1 || *req.PartitionCount > 1024 {
			writeError(w, http.StatusBadRequest, "partition_count must be between 1 and 1024")
			return
		}
		if l := c.layout.Load(); l != nil && l.Resharding != nil && *req.PartitionCount != cfg.PartitionCount {
			writeError(w, http.StatusConflict, "a resharding is already in progress; wait for it to finish")
			return
		}
		cfg.PartitionCount = *req.PartitionCount
	}
	if req.ReplicationFactor != nil {
		if *req.ReplicationFactor < 1 || *req.ReplicationFactor > 16 {
			writeError(w, http.StatusBadRequest, "replication_factor must be between 1 and 16")
			return
		}
		cfg.ReplicationFactor = *req.ReplicationFactor
	}
	if err := c.registry.PutConfig(r.Context(), cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	c.record("config changed: %d partitions, replication factor %d", cfg.PartitionCount, cfg.ReplicationFactor)
	c.kick()
	writeJSON(w, http.StatusOK, cfg)
}

// handleDrain marks a node for removal (its replicas and leaderships move elsewhere, after which
// its container can be stopped) or cancels that. Adding a node is simply starting it: it
// registers itself in etcd and the balancer gives it partitions.
func (c *Controller) handleDrain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Node    string `json:"node"`
		Drained bool   `json:"drained"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Node == "" {
		writeError(w, http.StatusBadRequest, "body must be {\"node\": ..., \"drained\": true|false}")
		return
	}
	if err := c.registry.SetDrained(r.Context(), req.Node, req.Drained); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if req.Drained {
		c.record("node %s is being drained: its partitions move to other nodes", req.Node)
	} else {
		c.record("node %s is no longer drained", req.Node)
	}
	c.kick()
	writeJSON(w, http.StatusOK, req)
}

type partitionRequest struct {
	Generation int64  `json:"generation"`
	Partition  int    `json:"partition"`
	Node       string `json:"node"`
	From       string `json:"from"`
	To         string `json:"to"`
}

func (c *Controller) partitionFor(l *model.Layout, req partitionRequest) (model.PartitionRef, *model.Partition, error) {
	ref := model.PartitionRef{Generation: req.Generation, ID: req.Partition}
	if ref.Generation == 0 {
		ref.Generation = l.Generation
	}
	part := l.Find(ref)
	if part == nil {
		return ref, nil, fmt.Errorf("partition %s does not exist", ref)
	}
	return ref, part, nil
}

// handleTransferLeader hands leadership of a partition to one of its in-sync followers.
func (c *Controller) handleTransferLeader(w http.ResponseWriter, r *http.Request) {
	var req partitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Node == "" {
		writeError(w, http.StatusBadRequest, "body must be {\"partition\": N, \"node\": ...}")
		return
	}
	events, err := c.updateLayout(r.Context(), func(l *model.Layout) ([]string, error) {
		ref, part, err := c.partitionFor(l, req)
		if err != nil {
			return nil, err
		}
		switch {
		case part.Leader == req.Node:
			return nil, nil
		case part.State == model.PartitionHandoff:
			return nil, fmt.Errorf("partition %s is already handing off to %s", ref, part.HandoffTo)
		case !model.ContainsString(part.Followers, req.Node):
			return nil, fmt.Errorf("%s is not an in-sync follower of %s; move a replica there first", req.Node, ref)
		}
		p := &planner{l: l, in: planInput{Now: time.Now()}}
		p.startHandoff(ref, part, req.Node, "requested from the panel; partition pinned")
		part.Pinned = true
		return p.events, nil
	})
	c.respondLayoutChange(w, events, err)
}

// handleMove moves one replica of a partition to another node: the new copy joins first and
// the old one is removed once the new one is in sync (with a leadership handoff if needed).
func (c *Controller) handleMove(w http.ResponseWriter, r *http.Request) {
	var req partitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.From == "" || req.To == "" {
		writeError(w, http.StatusBadRequest, "body must be {\"partition\": N, \"from\": ..., \"to\": ...}")
		return
	}
	events, err := c.updateLayout(r.Context(), func(l *model.Layout) ([]string, error) {
		ref, part, err := c.partitionFor(l, req)
		if err != nil {
			return nil, err
		}
		switch {
		case !part.Has(req.From):
			return nil, fmt.Errorf("%s holds no replica of %s", req.From, ref)
		case part.Has(req.To):
			return nil, fmt.Errorf("%s already holds a replica of %s", req.To, ref)
		case l.Members[req.To] == 0:
			return nil, fmt.Errorf("%s is not a live member of the cluster", req.To)
		}
		part.Joining = append(part.Joining, req.To)
		if !model.ContainsString(part.Evict, req.From) {
			part.Evict = append(part.Evict, req.From)
		}
		part.Pinned = true
		return []string{fmt.Sprintf("partition %s: moving replica %s -> %s (requested from the panel; partition pinned)", ref, req.From, req.To)}, nil
	})
	c.respondLayoutChange(w, events, err)
}

// handleUnpin hands a manually placed partition back to the auto-balancer.
func (c *Controller) handleUnpin(w http.ResponseWriter, r *http.Request) {
	var req partitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "body must be {\"partition\": N}")
		return
	}
	events, err := c.updateLayout(r.Context(), func(l *model.Layout) ([]string, error) {
		ref, part, err := c.partitionFor(l, req)
		if err != nil || !part.Pinned {
			return nil, err
		}
		part.Pinned = false
		return []string{fmt.Sprintf("partition %s unpinned: the auto-balancer may move it again", ref)}, nil
	})
	c.respondLayoutChange(w, events, err)
}

func (c *Controller) respondLayoutChange(w http.ResponseWriter, events []string, err error) {
	if err != nil {
		code := http.StatusConflict
		if errors.Is(err, errNotLeader) || errors.Is(err, registry.ErrConflict) {
			code = http.StatusServiceUnavailable
		}
		writeError(w, code, err.Error())
		return
	}
	for _, e := range events {
		c.record("%s", e)
	}
	c.kick()
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": events})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
