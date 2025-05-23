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

		n.mu.RLock()
		status := string(n.status)
		n.mu.RUnlock()

		data := map[string]string{
			"id":     n.ID,
			"status": status,
		}

		if err := n.networkClient.Post(url, data, nil); err != nil {
			log.Printf("Error sending heartbeat: %v", err)
		}
	}
}

// AddPartition adds a partition to this node
func (n *Node) AddPartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	p := partition.NewPartition(partitionID, model.NodeRoleFollower)
	n.partitions[partitionID] = p

	log.Printf("Node %s added partition %d", n.ID, partitionID)
}

// RemovePartition removes a partition from this node
func (n *Node) RemovePartition(partitionID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	delete(n.partitions, partitionID)
	log.Printf("Node %s removed partition %d", n.ID, partitionID)
}

// getPartitionForKey finds the right partition for a given key
func (n *Node) getPartitionForKey(key string) (*partition.Partition, error) {
	partitionID := hash.GetPartitionID(key, n.partitionCount)

	n.mu.RLock()
	defer n.mu.RUnlock()

	p, exists := n.partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found on this node", partitionID)
	}

	return p, nil
}

// Set adds or updates a key-value pair
func (n *Node) Set(key, value string) error {
	p, err := n.getPartitionForKey(key)
	if err != nil {
		return err
	}

	if entry := p.Set(key, value); entry != nil {
		n.mu.RLock()
		partitionID := p.ID
		n.mu.RUnlock()

		go n.notifyFollowers(partitionID, entry)
	}
	return nil
}

// Get retrieves a value by key
func (n *Node) Get(key string) (string, bool, error) {
	p, err := n.getPartitionForKey(key)
	if err != nil {
		return "", false, err
	}

	value, exists := p.Get(key)
	return value, exists, nil
}

// Delete removes a key-value pair
func (n *Node) Delete(key string) error {
	p, err := n.getPartitionForKey(key)
	if err != nil {
		return err
	}

	if entry := p.Delete(key); entry != nil {
		n.mu.RLock()
		partitionID := p.ID
		n.mu.RUnlock()

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

	p, err := n.getPartitionForKey(entry.Operation.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		n.AddPartition(data.PartitionID)
	case "remove":
		n.RemovePartition(data.PartitionID)
	case "become_leader":
		n.mu.RLock()
		p, exists := n.partitions[data.PartitionID]
		n.mu.RUnlock()

		if !exists {
			http.Error(w, "Partition not found", http.StatusNotFound)
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

	n.mu.RLock()
	p, exists := n.partitions[req.PartitionID]
	if !exists || p.Role != model.NodeRoleFollower {
		n.mu.RUnlock()
		http.Error(w, "Not a follower of this partition", http.StatusForbidden)
		return
	}
	n.mu.RUnlock()
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
			n.mu.Lock()
			n.partitions = make(map[int]*partition.Partition)
			n.nodes = make(map[string]*model.Node)
			n.mu.Unlock()

			log.Printf("Node %s removed from controller. Re-registering...", n.ID)
			n.registerWithController()
			continue
		}

		n.mu.Lock()
		n.nodes = nodeMap
		n.mu.Unlock()

		var toRemove []int

		n.mu.RLock()
		for id := range n.partitions {
			contains := model.ContainsString(partitionList[id].FollowerIDs, n.ID)
			if _, exists := partitionMap[id]; !exists || (!contains && partitionList[id].LeaderID != n.ID) {
				toRemove = append(toRemove, id)
			}
		}
		n.mu.RUnlock()

		for id := range toRemove {
			n.RemovePartition(id)
		}

		flag := false
		n.mu.RLock()
		for id := range partitionList {
			if _, exists := n.partitions[id]; !exists {
				flag = true
				break
			}
		}
		n.mu.RUnlock()

		if flag {
			n.mu.Lock()
			n.partitions = make(map[int]*partition.Partition)
			n.nodes = make(map[string]*model.Node)
			n.status = model.NodeStatusUnhealthy
			n.mu.Unlock()

			log.Printf("Node %s transitioning to unhealthy state due to inconsistent partitions", n.ID)
			time.Sleep(10 * time.Second)

			n.mu.Lock()
			n.status = model.NodeStatusHealthy
			n.mu.Unlock()

			log.Printf("Node %s recovered and is now healthy again", n.ID)
			n.registerWithController()
		}

		for id, partition := range partitionList {
			n.mu.RLock()
			if p, exists := n.partitions[id]; exists {
				p.UpdateFollowers(partition.FollowerIDs)
			}
			n.mu.RUnlock()
		}
	}
}

func (n *Node) notifyFollowers(partitionID int, entry *model.LogEntry) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, nodeID := range n.partitions[partitionID].Followers() {
		if nodeID == n.ID {
			continue
		}
		node := n.nodes[nodeID]
		partitionItems := n.partitions[partitionID].Items()

		n.mu.RUnlock()
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
		n.mu.RLock()
	}
}

func (n *Node) isLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, partition := range n.partitions {
		if partition.Role == model.NodeRoleLeader {
			return true
		}
	}
	return false
}
