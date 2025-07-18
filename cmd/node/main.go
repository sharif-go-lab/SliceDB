package main

import (
	"flag"
	"log"
	"strings"

	"github.com/sharif-go-lab/SliceDB/internal/node"
)

func main() {
	// Command line flags
	id := flag.String("id", "", "Node ID")
	addr := flag.String("addr", ":8081", "Node address")
	partitionCount := flag.Int("partitions", 10, "Number of partitions")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "comma separated etcd endpoints")
	flag.Parse()

	if *id == "" {
		log.Fatal("Node ID is required")
	}

	// Create and start node
	endpoints := strings.Split(*etcdEndpoints, ",")
	n := node.NewNode(*id, *addr, endpoints, *partitionCount)
	log.Printf("Starting node %s at %s with %d partitions", *id, *addr, *partitionCount)

	if err := n.Start(); err != nil {
		log.Fatalf("Node error: %v", err)
	}
}
