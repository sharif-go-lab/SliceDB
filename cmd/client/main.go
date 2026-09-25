package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sharif-go-lab/SliceDB/pkg/client"
)

const usage = `usage: client [-lb URL[,URL...]] <command> [flags] [args]

commands:
  set <key> <value>      write a key
  get <key>              read a key
  del <key>              delete a key
  bench [flags]          load test: concurrent random reads/writes, prints RPS and latency
  fill [flags]           write -n deterministic keys (for checking data survives failures)
  verify [flags]         read those keys back and report missing / wrong values

The load balancer list defaults to $SLICEDB_LB or http://localhost:9000.
`

func main() {
	defaultLB := os.Getenv("SLICEDB_LB")
	if defaultLB == "" {
		defaultLB = "http://localhost:9000"
	}
	lb := flag.String("lb", defaultLB, "comma separated load balancer URLs")
	retries := flag.Int("retries", 8, "retries per operation")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	c := client.New(strings.Split(*lb, ","))
	c.MaxRetries = *retries
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cmd, args := flag.Arg(0), flag.Args()[1:]
	var err error
	switch cmd {
	case "set":
		err = requireArgs(args, 2, func() error { return c.Set(ctx, args[0], args[1]) })
		if err == nil {
			fmt.Println("OK")
		}
	case "get":
		err = requireArgs(args, 1, func() error {
			v, err := c.Get(ctx, args[0])
			if err == nil {
				fmt.Println(v)
			}
			return err
		})
	case "del", "delete":
		err = requireArgs(args, 1, func() error { return c.Delete(ctx, args[0]) })
		if err == nil {
			fmt.Println("OK")
		}
	case "bench":
		err = bench(ctx, c, args)
	case "fill":
		err = fill(ctx, c, args)
	case "verify":
		err = verify(ctx, c, args)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func requireArgs(args []string, n int, fn func() error) error {
	if len(args) != n {
		return fmt.Errorf("expected %d argument(s)", n)
	}
	return fn()
}

// recorder collects latencies per reporting interval and for the whole run.
type recorder struct {
	mu            sync.Mutex
	interval, all []time.Duration
	okI, errI     int
	ok, errs      int
	notFound      int
	lastErr       string
}

func (r *recorder) add(d time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		r.errI++
		r.errs++
		r.lastErr = err.Error()
		return
	}
	if errors.Is(err, client.ErrNotFound) {
		r.notFound++
	}
	r.okI++
	r.ok++
	r.interval = append(r.interval, d)
	r.all = append(r.all, d)
}

func (r *recorder) flush() (ok, errs int, lat []time.Duration, lastErr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ok, errs, lat, lastErr = r.okI, r.errI, r.interval, r.lastErr
	r.okI, r.errI, r.interval, r.lastErr = 0, 0, nil, ""
	return
}

func percentiles(lat []time.Duration) string {
	if len(lat) == 0 {
		return "p50=-  p95=-  p99=-"
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	at := func(q float64) time.Duration { return lat[int(q*float64(len(lat)-1))] }
	return fmt.Sprintf("p50=%-8s p95=%-8s p99=%-8s", round(at(0.50)), round(at(0.95)), round(at(0.99)))
}

func round(d time.Duration) time.Duration {
	if d > time.Millisecond {
		return d.Round(100 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

func bench(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	duration := fs.Duration("duration", 30*time.Second, "how long to run")
	concurrency := fs.Int("concurrency", 32, "concurrent workers")
	keys := fs.Int("keys", 10000, "size of the key space")
	reads := fs.Float64("reads", 0.8, "fraction of operations that are reads (the rest are writes)")
	deletes := fs.Float64("deletes", 0.0, "fraction of operations that are deletes")
	valueSize := fs.Int("value-size", 32, "value size in bytes")
	interval := fs.Duration("interval", time.Second, "reporting interval")
	prefix := fs.String("prefix", "bench", "key prefix")
	_ = fs.Parse(args)

	value := strings.Repeat("v", *valueSize)
	fmt.Printf("bench: %d workers, %s, %d keys, %.0f%% reads, %.0f%% deletes, %dB values\n",
		*concurrency, *duration, *keys, *reads*100, *deletes*100, *valueSize)

	runCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	rec := &recorder{}
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for runCtx.Err() == nil {
				key := fmt.Sprintf("%s-%d", *prefix, rng.Intn(*keys))
				start := time.Now()
				var err error
				switch x := rng.Float64(); {
				case x < *reads:
					_, err = c.Get(runCtx, key)
				case x < *reads+*deletes:
					err = c.Delete(runCtx, key)
				default:
					err = c.Set(runCtx, key, value)
				}
				if runCtx.Err() != nil {
					return
				}
				rec.add(time.Since(start), err)
			}
		}(time.Now().UnixNano() + int64(w))
	}

	start := time.Now()
	ticker := time.NewTicker(*interval)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for running := true; running; {
		select {
		case <-ticker.C:
		case <-done:
			running = false
		}
		ok, errs, lat, lastErr := rec.flush()
		line := fmt.Sprintf("[%6s] rps=%-7.0f ok=%-6d err=%-4d %s", time.Since(start).Round(time.Second), float64(ok)/interval.Seconds(), ok, errs, percentiles(lat))
		if lastErr != "" {
			line += "  last error: " + lastErr
		}
		if running {
			fmt.Println(line)
		}
	}
	ticker.Stop()

	elapsed := time.Since(start)
	fmt.Println("---")
	fmt.Printf("total: %d ok (%d not found), %d errors, %d client retries in %s\n", rec.ok, rec.notFound, rec.errs, c.Retries(), elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.0f req/s   latency %s\n", float64(rec.ok)/elapsed.Seconds(), percentiles(rec.all))
	return nil
}

func valueFor(prefix string, i int, tag string) string {
	return fmt.Sprintf("%s-value-%d-%s", prefix, i, tag)
}

// parallel runs fn(i) for i in [0, n) on workers goroutines.
func parallel(n, workers int, fn func(i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
}

func fill(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("fill", flag.ExitOnError)
	n := fs.Int("n", 2000, "number of keys")
	prefix := fs.String("prefix", "key", "key prefix")
	tag := fs.String("tag", "v1", "tag embedded in values (change it to overwrite)")
	workers := fs.Int("concurrency", 16, "concurrent writers")
	_ = fs.Parse(args)

	start := time.Now()
	var failed atomic.Int64
	parallel(*n, *workers, func(i int) {
		if err := c.Set(ctx, fmt.Sprintf("%s-%d", *prefix, i), valueFor(*prefix, i, *tag)); err != nil {
			failed.Add(1)
			fmt.Fprintf(os.Stderr, "set %s-%d: %v\n", *prefix, i, err)
		}
	})
	fmt.Printf("fill: wrote %d/%d keys (%s-0 .. %s-%d) in %s, %d retries\n",
		*n-int(failed.Load()), *n, *prefix, *prefix, *n-1, time.Since(start).Round(time.Millisecond), c.Retries())
	if failed.Load() > 0 {
		return fmt.Errorf("%d writes failed", failed.Load())
	}
	return nil
}

func verify(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	n := fs.Int("n", 2000, "number of keys")
	prefix := fs.String("prefix", "key", "key prefix")
	tag := fs.String("tag", "v1", "tag the values were written with")
	workers := fs.Int("concurrency", 16, "concurrent readers")
	_ = fs.Parse(args)

	start := time.Now()
	var ok, missing, wrong, failed atomic.Int64
	parallel(*n, *workers, func(i int) {
		key := fmt.Sprintf("%s-%d", *prefix, i)
		v, err := c.Get(ctx, key)
		switch {
		case errors.Is(err, client.ErrNotFound):
			missing.Add(1)
			fmt.Fprintf(os.Stderr, "missing: %s\n", key)
		case err != nil:
			failed.Add(1)
			fmt.Fprintf(os.Stderr, "get %s: %v\n", key, err)
		case v != valueFor(*prefix, i, *tag):
			wrong.Add(1)
			fmt.Fprintf(os.Stderr, "wrong value for %s: %q\n", key, v)
		default:
			ok.Add(1)
		}
	})
	fmt.Printf("verify: %d ok, %d missing, %d wrong, %d errors out of %d keys (%s)\n",
		ok.Load(), missing.Load(), wrong.Load(), failed.Load(), *n, time.Since(start).Round(time.Millisecond))
	if ok.Load() != int64(*n) {
		return errors.New("verification failed")
	}
	return nil
}
