package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/loadbalancer"
)

func main() {
	addr := flag.String("addr", ":9000", "Load balancer listen address")
	etcdEndpoints := flag.String("etcd", envOr("SLICEDB_ETCD", "localhost:2379"), "comma separated etcd endpoints")
	strategy := flag.String("read-strategy", envOr("SLICEDB_READ_STRATEGY", loadbalancer.StrategyRoundRobin),
		"where reads go: leader | round-robin | followers")
	attempts := flag.Int("attempts", 8, "attempts per request before giving up")
	timeout := flag.Duration("timeout", 3*time.Second, "timeout of a single request to a node")
	flag.Parse()

	lb, err := loadbalancer.New(loadbalancer.Options{
		Listen:       *addr,
		Etcd:         strings.Split(*etcdEndpoints, ","),
		ReadStrategy: *strategy,
		MaxAttempts:  *attempts,
		Timeout:      *timeout,
	})
	if err != nil {
		log.Fatalf("Load balancer error: %v", err)
	}
	if err := lb.Start(); err != nil {
		log.Fatalf("Load balancer error: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
