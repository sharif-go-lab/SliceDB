package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Counters is a set of named, monotonically increasing counters.
type Counters struct {
	values sync.Map // string -> *atomic.Int64
}

func NewCounters() *Counters {
	return &Counters{}
}

func (c *Counters) Add(name string, delta int64) {
	v, ok := c.values.Load(name)
	if !ok {
		v, _ = c.values.LoadOrStore(name, new(atomic.Int64))
	}
	v.(*atomic.Int64).Add(delta)
}

func (c *Counters) Inc(name string) {
	c.Add(name, 1)
}

func (c *Counters) Snapshot() map[string]int64 {
	out := make(map[string]int64)
	c.values.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// RateTracker turns successive counter snapshots into per-second rates.
type RateTracker struct {
	mu    sync.Mutex
	last  map[string]int64
	at    time.Time
	rates map[string]float64
}

func (t *RateTracker) Update(snapshot map[string]int64) map[string]float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	rates := make(map[string]float64)
	if !t.at.IsZero() {
		elapsed := now.Sub(t.at).Seconds()
		for k, v := range snapshot {
			rates[k] = float64(v-t.last[k]) / elapsed
		}
	}
	t.last, t.at, t.rates = snapshot, now, rates
	return rates
}

func (t *RateTracker) Rates() map[string]float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.rates
}
