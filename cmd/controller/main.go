package main

import (
	"flag"
	"log"

	"github.com/sharif-go-lab/SliceDB/internal/controller"
)

func main() {
	// Command line flags
	addr := flag.String("addr", ":8080", "Controller address")
	partitionCount := flag.Int("partitions", 10, "Number of partitions")
	replicationFactor := flag.Int("replication", 3, "Replication factor")
	flag.Parse()

	// Create and start controller
	ctrl := controller.NewController(*partitionCount, *replicationFactor)
	log.Printf("Starting controller with %d partitions and replication factor %d", *partitionCount, *replicationFactor)

	if err := ctrl.Start(*addr); err != nil {
		log.Fatalf("Controller error: %v", err)
	}
}
