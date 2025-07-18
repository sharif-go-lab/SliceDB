package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
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
	registry          *registry.EtcdRegistry
}

// NewController creates a new controller
func NewController(partitionCount, replicationFactor int, etcdEndpoints []string) *Controller {
	reg, err := registry.NewEtcdRegistry(etcdEndpoints)
	if err != nil {
		log.Fatalf("failed to connect to etcd: %v", err)
	}

	return &Controller{
		nodes:             make(map[string]*model.Node),
		partitions:        make(map[int]*model.Partition),
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		networkClient:     network.NewClient(),
		registry:          reg,
	}
}

// Start begins the controller operation
func (c *Controller) Start(addr string) error {
	// Initialize partitions from etcd if possible
	if err := c.initializePartitions(); err != nil {
		return err
	}

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
func (c *Controller) initializePartitions() error {
	parts, err := c.registry.GetPartitions(context.Background())
	if err == nil && len(parts) == c.partitionCount {
		c.mu.Lock()
		for _, p := range parts {
			cp := p
			c.partitions[p.ID] = &cp
		}
		c.mu.Unlock()
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.partitionCount; i++ {
		p := &model.Partition{ID: i, LeaderID: "", FollowerIDs: make([]string, 0)}
		c.partitions[i] = p
		_ = c.registry.SetPartition(context.Background(), *p)
	}
	return nil
}

// registerNode adds a new node to the cluster
func (c *Controller) registerNode(id, address string) {
	healthURL := fmt.Sprintf("http://%s/health", address)

	if err := model.RetryJob(func() error {
		var healthResp map[string]interface{}
		return c.networkClient.Post(healthURL, nil, &healthResp)
	}); err == nil {
		node := model.Node{
			ID:       id,
			Address:  address,
			Status:   model.NodeStatusHealthy,
			LastSeen: time.Now(),
		}
		c.mu.Lock()
		c.nodes[id] = &node
		c.mu.Unlock()
		_, _ = c.registry.RegisterNode(context.Background(), node, 15)

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
		nodes, err := c.registry.GetNodes(context.Background())
		if err != nil {
			log.Printf("failed to query nodes from etcd: %v", err)
			continue
		}
		c.mu.Lock()
		c.nodes = make(map[string]*model.Node)
		for _, n := range nodes {
			nCopy := n
			c.nodes[n.ID] = &nCopy
		}
		c.mu.Unlock()
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
				_ = c.registry.SetPartition(context.Background(), *c.partitions[partition.ID])
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
			_ = c.registry.SetPartition(context.Background(), *c.partitions[partition.ID])
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
			_ = c.registry.SetPartition(context.Background(), *c.partitions[partition.ID])
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

// notifyNodeRemovePartition sends a remove partition notification to a node
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
	list, err := c.registry.GetNodes(context.Background())
	if err != nil {
		log.Printf("failed to query nodes: %v", err)
		return nil
	}
	return list
}

func (c *Controller) getNode(nodeID string) (model.Node, bool) {
	nodes, err := c.registry.GetNodes(context.Background())
	if err != nil {
		return model.Node{}, false
	}
	for _, n := range nodes {
		if n.ID == nodeID {
			return n, true
		}
	}
	return model.Node{}, false
}

func (c *Controller) getPartitions() (partitions []model.Partition) {
	parts, err := c.registry.GetPartitions(context.Background())
	if err != nil {
		log.Printf("failed to query partitions: %v", err)
		return nil
	}
	return parts
}

func (c *Controller) getPartition(partitionID int) (model.Partition, bool) {
	parts, err := c.registry.GetPartitions(context.Background())
	if err != nil {
		return model.Partition{}, false
	}
	for _, p := range parts {
		if p.ID == partitionID {
			return p, true
		}
	}
	return model.Partition{}, false
}
