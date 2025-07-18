package main

import (
	"flag"
	"log"

	"github.com/sharif-go-lab/SliceDB/internal/controller"
)

func main() {
	// Command line flags
	id := flag.String("id", "controller", "Controller ID")
	addr := flag.String("addr", ":8080", "Controller address")
	partitionCount := flag.Int("partitions", 10, "Number of partitions")
	replicationFactor := flag.Int("replication", 3, "Replication factor")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "comma separated etcd endpoints")
	flag.Parse()

	// Create and start controller
	ctrl := controller.NewController(*id, *partitionCount, *replicationFactor, *etcdEndpoints)
	log.Printf("Starting controller %s with %d partitions and replication factor %d", *id, *partitionCount, *replicationFactor)

	if err := ctrl.Start(*addr); err != nil {
		log.Fatalf("Controller error: %v", err)
	}
}
