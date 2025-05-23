package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
	"github.com/sharif-go-lab/SliceDB/pkg/network"
)

// Controller manages the cluster nodes and partitions
type Controller struct {
	nodes             map[string]*model.Node
	partitions        map[int]*model.Partition
	partitionCount    int
	replicationFactor int
	mu                sync.RWMutex
	networkClient     *network.Client
}

// NewController creates a new controller
func NewController(partitionCount, replicationFactor int) *Controller {
	return &Controller{
		nodes:             make(map[string]*model.Node),
		partitions:        make(map[int]*model.Partition),
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		networkClient:     network.NewClient(),
	}
}

// Start begins the controller operation
func (c *Controller) Start(addr string) error {
	// Initialize partitions
	c.initializePartitions()

	// Start heartbeat checker
	go c.checkHeartbeats()

	// Set up HTTP handlers
	http.HandleFunc("/set", c.handleSet)
	http.HandleFunc("/get", c.handleGet)
	http.HandleFunc("/delete", c.handleDelete)
	http.HandleFunc("/register", c.handleRegister)
	http.HandleFunc("/heartbeat", c.handleHeartbeat)
	http.HandleFunc("/node-list", c.handleNodeList)
	http.HandleFunc("/partition-list", c.handlePartitionList)
	http.HandleFunc("/health", c.handleHealth)

	log.Printf("Controller starting on %s", addr)
	return http.ListenAndServe(addr, nil)
}

// initializePartitions creates the initial partition map without assigning nodes
func (c *Controller) initializePartitions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.partitionCount; i++ {
		c.partitions[i] = &model.Partition{
			ID:          i,
			LeaderID:    "",
			FollowerIDs: make([]string, 0),
		}
	}
}

// registerNode adds a new node to the cluster
func (c *Controller) registerNode(id, address string) {
	healthURL := fmt.Sprintf("http://%s/health", address)

	if err := model.RetryJob(func() error {
		var healthResp map[string]interface{}
		return c.networkClient.Post(healthURL, nil, &healthResp)
	}); err == nil {
		c.mu.Lock()
		c.nodes[id] = &model.Node{
			ID:       id,
			Address:  address,
			Status:   model.NodeStatusHealthy,
			LastSeen: time.Now(),
		}
		c.mu.Unlock()

		log.Printf("Node %s registered at %s", id, address)
		return
	}
}

// updateNodeHeartbeat updates the last seen time for a node
func (c *Controller) updateNodeHeartbeat(id string, status model.NodeStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[id]
	if !exists {
		return fmt.Errorf("node %s not found", id)
	}

	node.Status = status
	node.LastSeen = time.Now()

	return nil
}

// checkHeartbeats periodically checks node heartbeats
func (c *Controller) checkHeartbeats() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		unhealthyNodes := make([]string, 0)
		now := time.Now()

		// Check for unhealthy nodes
		c.mu.Lock()
		for id, node := range c.nodes {
			if node.Status == model.NodeStatusUnhealthy {
				unhealthyNodes = append(unhealthyNodes, id)
			} else if now.Sub(node.LastSeen) > 15*time.Second {
				log.Printf("Node %s marked as unhealthy", id)
				node.Status = model.NodeStatusUnhealthy
				unhealthyNodes = append(unhealthyNodes, id)
			}
		}
		c.mu.Unlock()

		// Handle failover for unhealthy nodes
		if len(unhealthyNodes) > 0 {
			c.handleFailover(unhealthyNodes)
		}

		// Rebalance partitions if needed
		c.rebalancePartitions()
	}
}

// handleFailover manages failover when nodes become unhealthy
func (c *Controller) handleFailover(unhealthyNodeIDs []string) {
	// Find partitions where an unhealthy node is a leader
	c.mu.Lock()
	for _, partition := range c.partitions {
		// Remove unhealthy nodes from followers if present
		var updatedFollowers []string
		for _, id := range partition.FollowerIDs {
			if !model.ContainsString(unhealthyNodeIDs, id) {
				updatedFollowers = append(updatedFollowers, id)
			}
		}
		partition.FollowerIDs = updatedFollowers

		if model.ContainsString(unhealthyNodeIDs, partition.LeaderID) {
			partition.LeaderID = ""
		}
	}
	c.mu.Unlock()
}

// rebalancePartitions distributes partitions among available nodes
func (c *Controller) rebalancePartitions() {
	// Get healthy nodes
	healthyNodes := make([]string, 0)
	for _, node := range c.getNodes() {
		if node.Status == model.NodeStatusHealthy {
			healthyNodes = append(healthyNodes, node.ID)
		}
	}

	if len(healthyNodes) == 0 {
		log.Printf("No healthy nodes available for rebalancing")
		return
	}

	// Assign partitions to nodes
	for _, partition := range c.getPartitions() {
		// Ensure we have enough followers
		currentFollowers := len(partition.FollowerIDs)
		desiredFollowers := c.replicationFactor
		if partition.LeaderID != "" {
			desiredFollowers -= 1
		}

		// If we need more followers
		if currentFollowers < desiredFollowers {
			// Find healthy nodes not already leaders or followers
			availableNodes := make([]string, 0)
			for _, nodeID := range healthyNodes {
				if nodeID != partition.LeaderID && !model.ContainsString(partition.FollowerIDs, nodeID) {
					availableNodes = append(availableNodes, nodeID)
				}
			}

			// Calculate how many more followers we need
			neededFollowers := desiredFollowers - currentFollowers

			// Add new followers
			for i := 0; neededFollowers > 0 && i < len(availableNodes); i++ {
				node, _ := c.getNode(availableNodes[i])

				if err := model.RetryJob(func() error {
					return c.notifyNodeAddPartition(node, partition.ID)
				}); err != nil {
					log.Printf("Failed to add follower for partition %d to node %s", partition.ID, node.ID)
					continue
				}

				neededFollowers--
				c.mu.Lock()
				c.partitions[partition.ID].FollowerIDs = append(c.partitions[partition.ID].FollowerIDs, node.ID)
				c.mu.Unlock()

				log.Printf("Rebalance: Assigned follower for partition %d to node %s", partition.ID, node.ID)
			}
		}

		// If we have too many followers, remove excess
		if currentFollowers > desiredFollowers {
			// Keep the first N followers
			for _, followerID := range partition.FollowerIDs[desiredFollowers:] {
				if err := model.RetryJob(func() error {
					node, _ := c.getNode(followerID)
					return c.notifyNodeRemovePartition(node, partition.ID)
				}); err != nil {
					log.Printf("Failed to remove follower %s from partition %d", followerID, partition.ID)
				}
			}

			c.mu.Lock()
			c.partitions[partition.ID].FollowerIDs = c.partitions[partition.ID].FollowerIDs[:desiredFollowers]
			c.mu.Unlock()
		}

		// If no leader, assign one
		if partition.LeaderID == "" {
			if len(partition.FollowerIDs) == 0 {
				log.Printf("Warning: No followers available for partition %d", partition.ID)
				continue
			}
			newLeaderID := partition.FollowerIDs[rand.Intn(len(healthyNodes))]

			// Update partition leader
			var updatedFollowers []string
			for _, id := range partition.FollowerIDs {
				if id != newLeaderID {
					updatedFollowers = append(updatedFollowers, id)
				}
			}

			c.mu.Lock()
			c.partitions[partition.ID].LeaderID = newLeaderID
			c.partitions[partition.ID].FollowerIDs = updatedFollowers
			c.mu.Unlock()

			// Notify the new leader node
			if err := c.notifyNodeBecomingLeader(newLeaderID, partition.ID); err != nil {
				panic(err)
			}

			log.Printf("Rebalance: Assigned leader for partition %d to node %s", partition.ID, newLeaderID)
		}
	}
}

// notifyNodeBecomingLeader sends a role change notification to a node
func (c *Controller) notifyNodeBecomingLeader(nodeID string, partitionID int) error {
	if _, exists := c.getPartition(partitionID); !exists {
		log.Printf("Error: Partition %d not found for add partition notification", partitionID)
		return fmt.Errorf("partition not found")
	}
	node, exists := c.getNode(nodeID)
	if !exists {
		log.Printf("Error: Node %s not found for role change notification", nodeID)
		return fmt.Errorf("node not found")
	}

	url := fmt.Sprintf("http://%s/partition-update", node.Address)
	data := map[string]interface{}{
		"action":       "become_leader",
		"partition_id": partitionID,
	}

	if err := c.networkClient.Post(url, data, nil); err != nil {
		log.Printf("Error notifying node %s of role change: %v", nodeID, err)
		return err
	}
	return nil
}

// notifyNodeAddPartition sends a add partition notification to a node
func (c *Controller) notifyNodeAddPartition(node model.Node, partitionID int) error {
	if _, exists := c.getPartition(partitionID); !exists {
		log.Printf("Error: Partition %d not found for add partition notification", partitionID)
		return fmt.Errorf("partition not found")
	}

	url := fmt.Sprintf("http://%s/partition-update", node.Address)
	data := map[string]interface{}{
		"action":       "add",
		"partition_id": partitionID,
	}

	if err := c.networkClient.Post(url, data, nil); err != nil {
		log.Printf("Error notifying node %s of adding partition %d: %v", node.ID, partitionID, err)
		return err
	}
	return nil
}

// notifyNodeRemovePartition sends a add partition notification to a node
func (c *Controller) notifyNodeRemovePartition(node model.Node, partitionID int) error {
	if _, exists := c.getPartition(partitionID); !exists {
		log.Printf("Error: Partition %d not found for remove partition notification", partitionID)
		return fmt.Errorf("partition not found")
	}

	url := fmt.Sprintf("http://%s/partition-update", node.Address)
	data := map[string]interface{}{
		"action":       "remove",
		"partition_id": partitionID,
	}

	if err := c.networkClient.Post(url, data, nil); err != nil {
		log.Printf("Error notifying node %s of removing partition %d: %v", node.ID, partitionID, err)
		return err
	}
	return nil
}

// HTTP handlers
func (c *Controller) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	data.Address = data.ID + data.Address

	go c.registerNode(data.ID, data.Address)

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var status model.NodeStatus
	if data.Status == "healthy" {
		status = model.NodeStatusHealthy
	} else {
		status = model.NodeStatusUnhealthy
	}

	if err := c.updateNodeHeartbeat(data.ID, status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleNodeList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.getNodes())
}

func (c *Controller) handlePartitionList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(c.getPartitions())
}

func (c *Controller) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (c *Controller) handleSet(w http.ResponseWriter, r *http.Request) {
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

	partitionID := hash.GetPartitionID(data.Key, c.partitionCount)

	partition, exists := c.getPartition(partitionID)
	if !exists {
		http.Error(w, "Partition not found", http.StatusInternalServerError)
		return
	}

	leaderNode, exists := c.getNode(partition.LeaderID)
	if !exists {
		http.Error(w, "Leader node not found", http.StatusInternalServerError)
		return
	}

	url := fmt.Sprintf("http://%s/set", leaderNode.Address)

	if err := c.networkClient.Post(url, data, nil); err != nil {
		http.Error(w, fmt.Sprintf("Failed to forward request to leader: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter required", http.StatusBadRequest)
		return
	}

	partitionID := hash.GetPartitionID(key, c.partitionCount)

	partition, exists := c.getPartition(partitionID)
	if !exists {
		http.Error(w, "Partition not found", http.StatusInternalServerError)
		return
	}

	leaderNode, exists := c.getNode(partition.LeaderID)
	if !exists {
		http.Error(w, "Leader node not found", http.StatusInternalServerError)
		return
	}

	var response map[string]interface{}
	url := fmt.Sprintf("http://%s/get?key=%s", leaderNode.Address, key)

	if err := c.networkClient.Get(url, &response); err != nil {
		if err.Error() == "received non-200 response: 404" {
			http.Error(w, "Key not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("Failed to forward request to leader: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (c *Controller) handleDelete(w http.ResponseWriter, r *http.Request) {
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

	partitionID := hash.GetPartitionID(data.Key, c.partitionCount)

	partition, exists := c.getPartition(partitionID)
	if !exists {
		http.Error(w, "Partition not found", http.StatusInternalServerError)
		return
	}

	leaderNode, exists := c.getNode(partition.LeaderID)
	if !exists {
		http.Error(w, "Leader node not found", http.StatusInternalServerError)
		return
	}

	url := fmt.Sprintf("http://%s/delete", leaderNode.Address)

	if err := c.networkClient.Delete(url, data, nil); err != nil {
		http.Error(w, fmt.Sprintf("Failed to forward request to leader: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *Controller) getNodes() (nodes []model.Node) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, node := range c.nodes {
		nodes = append(nodes, *node)
	}
	return
}

func (c *Controller) getNode(nodeID string) (model.Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node, exists := c.nodes[nodeID]
	return *node, exists
}

func (c *Controller) getPartitions() (partitions []model.Partition) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, partition := range c.partitions {
		partitions = append(partitions, *partition)
	}
	return
}

func (c *Controller) getPartition(partitionID int) (model.Partition, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	partition, exists := c.partitions[partitionID]
	return *partition, exists
}
