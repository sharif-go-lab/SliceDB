package main

import (
	"flag"
	"log"

	"github.com/sharif-go-lab/SliceDB/internal/node"
)

func main() {
	// Command line flags
	id := flag.String("id", "", "Node ID")
	addr := flag.String("addr", ":8081", "Node address")
	partitionCount := flag.Int("partitions", 10, "Number of partitions")
	controllerAddr := flag.String("controller", "localhost:8080", "Controller address")
	flag.Parse()

	if *id == "" {
		log.Fatal("Node ID is required")
	}

	// Create and start node
	n := node.NewNode(*id, *addr, *controllerAddr, *partitionCount)
	log.Printf("Starting node %s at %s, controller at %s with %d partitions", *id, *addr, *controllerAddr, *partitionCount)

	if err := n.Start(); err != nil {
		log.Fatalf("Node error: %v", err)
	}
}
