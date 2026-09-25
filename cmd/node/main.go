package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sharif-go-lab/SliceDB/internal/node"
	"github.com/sharif-go-lab/SliceDB/internal/partition"
)

func main() {
	// Command line flags
	id := flag.String("id", "", "Node ID")
	addr := flag.String("addr", ":7000", "Node listen address")
	advertise := flag.String("advertise", "", "address other components use to reach this node (default: <id><addr>)")
	etcdEndpoints := flag.String("etcd", envOr("SLICEDB_ETCD", "localhost:2379"), "comma separated etcd endpoints")
	leaseTTL := flag.Int64("lease-ttl", 5, "etcd lease TTL in seconds (how fast a dead node is detected)")
	flushSize := flag.Int("flush-size", 4096, "memtable entries before it is frozen into an immutable level")
	maxLevels := flag.Int("max-levels", 4, "immutable levels before they are compacted into one")
	walRetain := flag.Int("wal-retain", 100000, "WAL entries kept per partition for lagging replicas")
	flag.Parse()

	if *id == "" {
		log.Fatal("Node ID is required")
	}
	if *advertise == "" && strings.HasPrefix(*addr, ":") {
		*advertise = *id + *addr
	} else if *advertise == "" {
		*advertise = *addr
	}

	// Create and start node
	n, err := node.NewNode(node.Options{
		ID:        *id,
		Listen:    *addr,
		Advertise: *advertise,
		Etcd:      strings.Split(*etcdEndpoints, ","),
		LeaseTTL:  *leaseTTL,
		Partition: partition.Options{FlushSize: *flushSize, MaxLevels: *maxLevels, WALRetain: *walRetain},
	})
	if err != nil {
		log.Fatalf("Node error: %v", err)
	}
	log.Printf("Starting node %s at %s", *id, *advertise)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := n.Start(ctx); err != nil {
		log.Fatalf("Node error: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
