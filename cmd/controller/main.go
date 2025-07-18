package main

import (
	"flag"
	"log"
	"strings"

	"github.com/sharif-go-lab/SliceDB/internal/controller"
)

func main() {
	// Command line flags
	addr := flag.String("addr", ":8080", "Controller address")
	partitionCount := flag.Int("partitions", 10, "Number of partitions")
	replicationFactor := flag.Int("replication", 3, "Replication factor")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "comma separated etcd endpoints")
	flag.Parse()

	// Create and start controller
	endpoints := strings.Split(*etcdEndpoints, ",")
	ctrl := controller.NewController(*partitionCount, *replicationFactor, endpoints, *addr)
	log.Printf("Starting controller with %d partitions and replication factor %d", *partitionCount, *replicationFactor)

	if err := ctrl.Start(*addr); err != nil {
		log.Fatalf("Controller error: %v", err)
	}
}
