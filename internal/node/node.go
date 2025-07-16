package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/partition"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
	"github.com/sharif-go-lab/SliceDB/pkg/network"
)

// Node represents a database node
type Node struct {
	ID             string
	Address        string
	ControllerAddr string
	partitions     map[int]*partition.Partition
	nodes          map[string]*model.Node
	partitionCount int
	mu             sync.RWMutex
	networkClient  *network.Client
	status         model.NodeStatus
}

// NewNode creates a new database node
func NewNode(id, address, controllerAddr string, partitionCount int) *Node {
	node := &Node{
		ID:             id,
		Address:        address,
		ControllerAddr: controllerAddr,
		partitions:     make(map[int]*partition.Partition),
		nodes:          make(map[string]*model.Node),
		partitionCount: partitionCount,
		networkClient:  network.NewClient(),
		status:         model.NodeStatusHealthy,
	}

	return node
}

// Start begins the node operation
func (n *Node) Start() error {
	// Register with controller
	if err := n.registerWithController(); err != nil {
		return fmt.Errorf("failed to register with controller: %v", err)
	}

	// Start heartbeat
	go n.startHeartbeat()

	// Start update data
	go n.updateData()

	// Start HTTP server for node communication
	http.HandleFunc("/set", n.handleSet)
	http.HandleFunc("/get", n.handleGet)
	http.HandleFunc("/delete", n.handleDelete)
	http.HandleFunc("/apply-log", n.handleApplyLog)
	http.HandleFunc("/sync-partition", n.handleSyncPartition)
	http.HandleFunc("/partition-update", n.handlePartitionUpdate)
	http.HandleFunc("/health", n.handleHealth)

	log.Printf("Node %s starting on %s", n.ID, n.Address)
	return http.ListenAndServe(n.Address, nil)
}

// registerWithController registers this node with the controller
func (n *Node) registerWithController() error {
	url := fmt.Sprintf("http://%s/register", n.ControllerAddr)

	data := map[string]string{
		"id":      n.ID,
		"address": n.Address,
	}

	return n.networkClient.Post(url, data, nil)
}

// startHeartbeat begins sending periodic heartbeats to the controller
func (n *Node) startHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		url := fmt.Sprintf("http://%s/heartbeat", n.ControllerAddr)

		data := map[string]string{
			"id":     n.ID,
			"status": string(n.getStatus()),
		}

		if err := n.networkClient.Post(url, data, nil); err != nil {
			log.Printf("Error sending heartbeat: %v", err)
		}
	}
}

// AddPartition adds a partition to this node
func (n *Node) addPartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions[partitionID] = partition.NewPartition(partitionID, model.NodeRoleFollower)
	log.Printf("Node %s added partition %d", n.ID, partitionID)
}

// RemovePartition removes a partition from this node
func (n *Node) removePartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.partitions, partitionID)
	log.Printf("Node %s removed partition %d", n.ID, partitionID)
}

// Set adds or updates a key-value pair
func (n *Node) Set(key, value string) error {
	partitionID := hash.GetPartitionID(key, n.partitionCount)
	if _, exists := n.getPartition(partitionID); !exists {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if entry := n.partitions[partitionID].Set(key, value); entry != nil {
		go n.notifyFollowers(partitionID, entry)
	}
	return nil
}

// Get retrieves a value by key
func (n *Node) Get(key string) (string, bool, error) {
	partitionID := hash.GetPartitionID(key, n.partitionCount)
	if _, exists := n.getPartition(partitionID); !exists {
		return "", false, fmt.Errorf("partition %d not found on this node", partitionID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	value, exists := n.partitions[partitionID].Get(key)
	return value, exists, nil
}

// Delete removes a key-value pair
func (n *Node) Delete(key string) error {
	partitionID := hash.GetPartitionID(key, n.partitionCount)
	if _, exists := n.getPartition(partitionID); !exists {
		return fmt.Errorf("partition %d not found on this node", partitionID)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if entry := n.partitions[partitionID].Delete(key); entry != nil {
		go n.notifyFollowers(partitionID, entry)
	}
	return nil
}

// HTTP handlers
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

	if err := n.Set(data.Key, data.Value); err != nil {
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

	value, exists, err := n.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	response := map[string]string{"value": value}
	json.NewEncoder(w).Encode(response)
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

	if err := n.Delete(data.Key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (n *Node) handleApplyLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var entry model.LogEntry
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	partitionID := hash.GetPartitionID(entry.Operation.Key, n.partitionCount)

	n.mu.Lock()
	defer n.mu.Unlock()

	p, exists := n.partitions[partitionID]
	if !exists {
		http.Error(w, fmt.Sprintf("partition %d not found on this node", partitionID), http.StatusInternalServerError)
		return
	}

	if err := p.ApplyLogEntry(entry); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
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

func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (n *Node) handleSyncPartition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PartitionID     int               `json:"partition_id"`
		Items           map[string]string `json:"items"`
		LastSequenceNum int64             `json:"last_sequence_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	p, exists := n.partitions[req.PartitionID]
	if !exists || p.Role != model.NodeRoleFollower {
		http.Error(w, "Not a follower of this partition", http.StatusForbidden)
		return
	}

	p.SyncItems(req.Items, req.LastSequenceNum)
	w.WriteHeader(http.StatusOK)
}

func (n *Node) updateData() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		if !n.isLeader() {
			continue
		}

		nodeList := make([]model.Node, 0)
		url := fmt.Sprintf("http://%s/node-list", n.ControllerAddr)

		if err := n.networkClient.Get(url, &nodeList); err != nil {
			log.Printf("Failed to fetch node list: %v", err)
			continue
		}

		nodeMap := make(map[string]*model.Node, 0)
		for _, node := range nodeList {
			nodeMap[node.ID] = &node
		}

		partitionList := make([]model.Partition, 0)
		url = fmt.Sprintf("http://%s/partition-list", n.ControllerAddr)

		if err := n.networkClient.Get(url, &partitionList); err != nil {
			log.Printf("Failed to fetch partition list: %v", err)
			continue
		}

		partitionMap := make(map[int]*model.Partition, 0)
		for _, partition := range partitionList {
			partitionMap[partition.ID] = &partition
		}

		if _, exists := nodeMap[n.ID]; !exists {
			n.setPartitions([]partition.Partition{})
			n.setNodes([]model.Node{})

			log.Printf("Node %s removed from controller. Re-registering...", n.ID)
			n.registerWithController()
			continue
		}

		n.setNodes(nodeList)

		var toRemove []int

		for _, partition := range n.getPartitions() {
			contains := model.ContainsString(partitionList[partition.ID].FollowerIDs, n.ID)
			if _, exists := partitionMap[partition.ID]; !exists || (!contains && partitionList[partition.ID].LeaderID != n.ID) {
				toRemove = append(toRemove, partition.ID)
			}
		}

		for _, id := range toRemove {
			n.removePartition(id)
		}

		flag := false
		for _, partition := range partitionList {
			if _, exists := n.getPartition(partition.ID); !exists {
				flag = true
				break
			}
		}

		if flag {
			n.setPartitions([]partition.Partition{})
			n.setNodes([]model.Node{})
			n.setStatus(model.NodeStatusUnhealthy)

			log.Printf("Node %s transitioning to unhealthy state due to inconsistent partitions", n.ID)
			time.Sleep(10 * time.Second)

			n.setStatus(model.NodeStatusHealthy)

			log.Printf("Node %s recovered and is now healthy again", n.ID)
			n.registerWithController()
		}

		for id, partition := range partitionList {
			n.mu.Lock()
			if p, exists := n.partitions[id]; exists {
				p.UpdateFollowers(partition.FollowerIDs)
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) notifyFollowers(partitionID int, entry *model.LogEntry) {
	partition, _ := n.getPartition(partitionID)
	for _, nodeID := range partition.Followers() {
		if nodeID == n.ID {
			continue
		}
		node, _ := n.getNode(nodeID)
		partitionItems := partition.Items()

		go func(nodeID, nodeAddress string) {
			url := fmt.Sprintf("http://%s/apply-log", nodeAddress)

			if err := n.networkClient.Post(url, entry, nil); err != nil {
				url := fmt.Sprintf("http://%s/sync-partition", nodeAddress)

				if err := n.networkClient.Post(url, map[string]interface{}{
					"partition_id":     partitionID,
					"items":            partitionItems,
					"last_sequence_id": entry.SequenceNumber,
				}, nil); err != nil {
					log.Printf("Failed to sync partition %d with node %s: %v", partitionID, nodeID, err)
				}
			}
		}(node.ID, node.Address)
	}
}

func (n *Node) isLeader() bool {
	for _, partition := range n.getPartitions() {
		if partition.Role == model.NodeRoleLeader {
			return true
		}
	}
	return false
}

func (n *Node) getStatus() model.NodeStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.status
}

func (n *Node) setStatus(status model.NodeStatus) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.status = status
}

func (n *Node) setNodes(nodes []model.Node) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nodes = make(map[string]*model.Node)
	for _, node := range nodes {
		n.nodes[node.ID] = &node
	}
}

func (n *Node) getNode(nodeID string) (model.Node, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	node, exists := n.nodes[nodeID]
	return *node, exists
}

func (n *Node) getPartition(partitionID int) (partition.Partition, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	partition, exists := n.partitions[partitionID]
	return *partition, exists
}

func (n *Node) setPartitions(partitions []partition.Partition) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.partitions = make(map[int]*partition.Partition)
	for _, partition := range partitions {
		n.partitions[partition.ID] = &partition
	}
}

func (n *Node) getPartitions() (partitions []partition.Partition) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, partition := range n.partitions {
		partitions = append(partitions, *partition)
	}
	return
}
