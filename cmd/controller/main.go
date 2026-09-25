package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/controller"
)

func main() {
	hostname, _ := os.Hostname()

	// Command line flags
	id := flag.String("id", hostname, "Controller ID")
	addr := flag.String("addr", ":8080", "Controller listen address")
	advertise := flag.String("advertise", "", "address other controllers use to reach this one (default: <id><addr>)")
	partitionCount := flag.Int("partitions", 8, "initial number of partitions (only used if etcd has no config yet)")
	replicationFactor := flag.Int("replication", 2, "initial number of copies per partition (only used if etcd has no config yet)")
	etcdEndpoints := flag.String("etcd", envOr("SLICEDB_ETCD", "localhost:2379"), "comma separated etcd endpoints")
	interval := flag.Duration("interval", time.Second, "control loop interval")
	autoBalance := flag.Bool("auto-balance", true, "move replicas and leaderships to keep nodes evenly loaded")
	sessionTTL := flag.Int("session-ttl", 5, "etcd session TTL in seconds (how fast a dead leader is replaced)")
	flag.Parse()

	if *advertise == "" && strings.HasPrefix(*addr, ":") {
		*advertise = *id + *addr
	} else if *advertise == "" {
		*advertise = *addr
	}

	ctrl, err := controller.NewController(controller.Options{
		ID:                *id,
		Listen:            *addr,
		Advertise:         *advertise,
		Etcd:              strings.Split(*etcdEndpoints, ","),
		PartitionCount:    *partitionCount,
		ReplicationFactor: *replicationFactor,
		Interval:          *interval,
		AutoBalance:       *autoBalance,
		SessionTTL:        *sessionTTL,
	})
	if err != nil {
		log.Fatalf("Controller error: %v", err)
	}
	log.Printf("Starting controller %s (initial config: %d partitions, replication factor %d)", *id, *partitionCount, *replicationFactor)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := ctrl.Start(ctx); err != nil {
		log.Fatalf("Controller error: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
