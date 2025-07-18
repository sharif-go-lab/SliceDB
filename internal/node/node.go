package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/partition"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
	"github.com/sharif-go-lab/SliceDB/pkg/network"
)

type Node struct {
	ID             string
	Address        string
	partitionCount int
	mu             sync.RWMutex
	networkClient  *network.Client
	partitions     map[int]*partition.Partition

	registry *registry.EtcdRegistry
	leaseID  clientv3.LeaseID
}

func NewNode(id, address string, etcdEndpoints []string, partitionCount int) *Node {
	reg, err := registry.NewEtcdRegistry(etcdEndpoints)
	if err != nil {
		log.Fatalf("failed to connect to etcd: %v", err)
	}

	node := &Node{
		ID:             id,
		Address:        address,
		registry:       reg,
		partitionCount: partitionCount,
		networkClient:  network.NewClient(),
		partitions:     make(map[int]*partition.Partition),
	}

	return node
}

func (n *Node) Start() error {
	// Register with etcd
	if err := n.registerWithEtcd(); err != nil {
		return fmt.Errorf("failed to register with etcd: %v", err)
	}

	// Start heartbeat
	go n.startHeartbeat()

	// Update partitions
	go n.updatePartitions()

	// Start HTTP server for node communication
	http.HandleFunc("/set", n.handleSet)
	http.HandleFunc("/get", n.handleGet)
	http.HandleFunc("/delete", n.handleDelete)
	http.HandleFunc("/health", n.handleHealth)
	http.HandleFunc("/get-logs-after", n.handleGetLogsAfter)
	http.HandleFunc("/partition-update", n.handlePartitionUpdate)

	log.Printf("Node %s starting on %s", n.ID, n.Address)
	return http.ListenAndServe(n.Address, nil)
}

func (n *Node) registerWithEtcd() error {
	node := model.Node{
		ID:      n.ID,
		Address: n.Address,
	}

	lease, err := n.registry.RegisterNode(context.Background(), node, 15)
	if err == nil {
		n.leaseID = lease
	}
	return err
}

func (n *Node) startHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		if err := n.registry.UpdateNodeHeartbeat(context.Background(), n.leaseID); err != nil {
			log.Printf("Error sending heartbeat: %v", err)
		}
	}
}

func (n *Node) updatePartitions() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		controllerUrl, err := n.registry.GetControllerAddress(context.Background())
		if err != nil {
			log.Printf("Failed to get controller address: %v", err)
			continue
		}

		for _, partition := range n.partitions {
			if partition.Role == model.NodeRoleLeader {
				continue
			}

			var logs []model.LogEntry
			url := fmt.Sprintf("%s/get-logs-after?seq=%d&partition=%d", controllerUrl, partition.SequenceNumber(), partition.ID)
			if err := n.networkClient.Get(url, &logs); err != nil {
				log.Printf("Failed to get logs for partition %d: %v", partition.ID, err)
				continue
			}

			if err := partition.ApplyLogEntries(logs); err != nil {
				log.Printf("Failed to apply logs for partition %d: %v", partition.ID, err)
				continue
			}
		}
	}
}

func (n *Node) addPartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions[partitionID] = partition.NewPartition(partitionID, model.NodeRoleFollower)
	log.Printf("Node %s added partition %d", n.ID, partitionID)
}

func (n *Node) removePartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.partitions, partitionID)
	log.Printf("Node %s removed partition %d", n.ID, partitionID)
}

func (n *Node) set(key, value string) error {
	partitionID := hash.GetPartitionID(key, n.partitionCount)
	if _, exists := n.getPartition(partitionID); !exists {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions[partitionID].Set(key, value)
	return nil
}

func (n *Node) get(key string) (string, bool, error) {
	partitionID := hash.GetPartitionID(key, n.partitionCount)
	if _, exists := n.getPartition(partitionID); !exists {
		return "", false, fmt.Errorf("partition %d not found on this node", partitionID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	value, exists := n.partitions[partitionID].Get(key)
	return value, exists, nil
}

func (n *Node) delete(key string) error {
	partitionID := hash.GetPartitionID(key, n.partitionCount)
	if _, exists := n.getPartition(partitionID); !exists {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions[partitionID].Delete(key)
	return nil
}

func (n *Node) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := n.set(data.Key, data.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter required", http.StatusBadRequest)
		return
	}

	value, exists, err := n.get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	response := map[string]string{"value": value}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Key string `json:"key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := n.delete(data.Key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (n *Node) handleGetLogsAfter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	seq := r.URL.Query().Get("seq")
	seqInt, err := strconv.ParseInt(seq, 10, 64)
	if err != nil {
		http.Error(w, "Invalid sequence number", http.StatusBadRequest)
		return
	}

	partition := r.URL.Query().Get("partition")
	partitionInt, err := strconv.Atoi(partition)
	if err != nil {
		http.Error(w, "Invalid partition id", http.StatusBadRequest)
		return
	}

	p, exists := n.partitions[partitionInt]
	if !exists {
		http.Error(w, fmt.Sprintf("partition %d not found on this node", partitionInt), http.StatusInternalServerError)
		return
	}

	logs := p.GetLogsAfter(seqInt)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(logs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (n *Node) handlePartitionUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Action      string `json:"action"`
		PartitionID int    `json:"partition_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch data.Action {
	case "add":
		n.addPartition(data.PartitionID)
	case "remove":
		n.removePartition(data.PartitionID)
	case "become_leader":
		n.mu.Lock()
		defer n.mu.Unlock()

		p, exists := n.partitions[data.PartitionID]
		if !exists {
			http.Error(w, fmt.Sprintf("partition %d not found on this node", data.PartitionID), http.StatusNotFound)
			return
		}
		p.ChangeRole(model.NodeRoleLeader)
	default:
		http.Error(w, "Invalid action", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (n *Node) isLeader() bool {
	for _, p := range n.getPartitions() {
		if p.Role == model.NodeRoleLeader {
			return true
		}
	}
	return false
}

func (n *Node) getNode(nodeID string) (*model.Node, bool) {
	nodes, err := n.registry.GetNodes(context.Background())
	if err != nil {
		return nil, false
	}
	for _, node := range nodes {
		if node.ID == nodeID {
			return &node, true
		}
	}
	return nil, false
}

func (n *Node) getPartition(partitionID int) (partition.Partition, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	p, exists := n.partitions[partitionID]
	if !exists {
		return partition.Partition{}, false
	}
	return *p, exists
}

func (n *Node) getPartitionFollowers(partitionID int) ([]string, error) {
	partitions, err := n.registry.GetPartitions(context.Background())
	if err != nil {
		return nil, err
	}
	for _, p := range partitions {
		if p.ID == partitionID {
			return p.FollowerIDs, nil
		}
	}
	return nil, fmt.Errorf("partition %d not found", partitionID)
}

func (n *Node) setPartitions(partitions []partition.Partition) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions = make(map[int]*partition.Partition)
	for _, p := range partitions {
		n.partitions[p.ID] = &p
	}
}

func (n *Node) getPartitions() (partitions []partition.Partition) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.partitions {
		partitions = append(partitions, *p)
	}
	return
}
