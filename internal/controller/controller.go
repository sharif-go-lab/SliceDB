package controller

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
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

	err := retryJob(500, 15, func() error {
		var healthResp map[string]interface{}
		return c.networkClient.Post(healthURL, nil, &healthResp)
	})
	if err == nil {
		node := &model.Node{
			ID:       id,
			Address:  address,
			Status:   model.NodeStatusHealthy,
			LastSeen: time.Now(),
		}

		// Register partitions in node
		for partitionID := range c.partitions {
			err := c.notifyNodeAddPartition(node, partitionID, model.NodeRoleFollower)
			if err != nil {
				log.Printf("Failed to registered partition %d to node %s", partitionID, id)
				return
			}
		}

		c.mu.Lock()
		c.nodes[id] = node
		c.mu.Unlock()

		log.Printf("Node %s registered at %s", id, address)
		c.rebalancePartitions()
		return
	}
}

// updateNodeHeartbeat updates the last seen time for a node
func (c *Controller) updateNodeHeartbeat(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.nodes[id]
	if !exists {
		return fmt.Errorf("node %s not found", id)
	}

	node.Status = model.NodeStatusHealthy
	node.LastSeen = time.Now()

	return nil
}

// checkHeartbeats periodically checks node heartbeats
func (c *Controller) checkHeartbeats() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		c.mu.Lock()

		unhealthyNodes := make([]string, 0)
		now := time.Now()

		// Check for unhealthy nodes
		for id, node := range c.nodes {
			if now.Sub(node.LastSeen) > 15*time.Second {
				if node.Status == model.NodeStatusHealthy {
					log.Printf("Node %s marked as unhealthy", id)
					node.Status = model.NodeStatusUnhealthy
					unhealthyNodes = append(unhealthyNodes, id)
				}
			}
		}

		c.mu.Unlock()

		// Handle failover for unhealthy nodes
		if len(unhealthyNodes) > 0 {
			c.handleFailover(unhealthyNodes)
		}
	}
}

// handleFailover manages failover when nodes become unhealthy
func (c *Controller) handleFailover(unhealthyNodeIDs []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find partitions where an unhealthy node is a leader
	for partitionID, partition := range c.partitions {
		// Remove unhealthy nodes from followers if present
		for _, nodeID := range unhealthyNodeIDs {
			var updatedFollowers []string
			for _, id := range partition.FollowerIDs {
				if id != nodeID {
					updatedFollowers = append(updatedFollowers, id)
				}
			}
			partition.FollowerIDs = updatedFollowers

			if partition.LeaderID == nodeID {
				// Choose a new leader from the followers
				if len(partition.FollowerIDs) > 0 {
					newLeaderID := partition.FollowerIDs[0]

					// Update partition information
					partition.LeaderID = newLeaderID

					// Remove new leader from followers
					var updatedFollowers []string
					for _, id := range partition.FollowerIDs {
						if id != newLeaderID {
							updatedFollowers = append(updatedFollowers, id)
						}
					}
					partition.FollowerIDs = updatedFollowers

					// Notify the new leader
					c.notifyNodeRoleChange(newLeaderID, partitionID, model.NodeRoleLeader)

					log.Printf("Failover: Node %s is the new leader for partition %d", newLeaderID, partitionID)
				} else {
					log.Printf("Warning: No followers available for partition %d after leader %s failure", partitionID, nodeID)
					partition.LeaderID = ""
				}
			}
		}
	}

	// Rebalance partitions if needed
	c.rebalancePartitions()
}

// rebalancePartitions distributes partitions among available nodes
func (c *Controller) rebalancePartitions() {
	// Get healthy nodes
	healthyNodes := make([]string, 0)
	for id, node := range c.nodes {
		if node.Status == model.NodeStatusHealthy {
			healthyNodes = append(healthyNodes, id)
		}
	}

	if len(healthyNodes) == 0 {
		log.Printf("No healthy nodes available for rebalancing")
		return
	}

	// Assign partitions to nodes
	for partitionID, partition := range c.partitions {
		// If no leader, assign one
		if partition.LeaderID == "" || c.nodes[partition.LeaderID].Status != model.NodeStatusHealthy {
			nodeIndex := partitionID % len(healthyNodes)
			newLeaderID := healthyNodes[nodeIndex]

			// Update partition leader
			oldLeaderID := partition.LeaderID
			partition.LeaderID = newLeaderID

			if oldLeaderID != newLeaderID {
				// Notify the new leader node
				c.notifyNodeRoleChange(newLeaderID, partitionID, model.NodeRoleLeader)
				log.Printf("Rebalance: Assigned leader for partition %d to node %s", partitionID, newLeaderID)
			}
		}

		// Ensure we have enough followers
		currentFollowers := len(partition.FollowerIDs)
		desiredFollowers := c.replicationFactor - 1 // -1 because leader is not counted

		// If we need more followers
		if currentFollowers < desiredFollowers {
			// Find healthy nodes not already leaders or followers
			availableNodes := make([]string, 0)
			for _, nodeID := range healthyNodes {
				if nodeID != partition.LeaderID && !containsString(partition.FollowerIDs, nodeID) {
					availableNodes = append(availableNodes, nodeID)
				}
			}

			// Calculate how many more followers we need
			neededFollowers := desiredFollowers - currentFollowers

			// Add new followers
			for i := 0; neededFollowers > 0 && i < len(availableNodes); i++ {
				node := c.nodes[availableNodes[i]]
				err := retryJob(500, 15, func() error {
					return c.notifyNodeAddPartition(node, partitionID, model.NodeRoleFollower)
				})
				if err == nil {
					neededFollowers--
					partition.FollowerIDs = append(partition.FollowerIDs, node.ID)

					// Notify the new follower
					c.notifyNodeRoleChange(node.ID, partitionID, model.NodeRoleFollower)
					log.Printf("Rebalance: Assigned follower for partition %d to node %s", partitionID, node.ID)
				}
			}
		}

		// If we have too many followers, remove excess
		if currentFollowers > desiredFollowers {
			// Keep the first N followers
			for _, followerID := range partition.FollowerIDs[desiredFollowers:] {
				retryJob(500, 15, func() error {
					return c.notifyNodeRemovePartition(c.nodes[followerID], partitionID)
				})
			}
			partition.FollowerIDs = partition.FollowerIDs[:desiredFollowers]
		}
	}
}

// notifyNodeRoleChange sends a role change notification to a node
func (c *Controller) notifyNodeRoleChange(nodeID string, partitionID int, role model.NodeRole) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, exists := c.partitions[partitionID]; !exists {
		log.Printf("Error: Partition %d not found for add partition notification", partitionID)
		return nil
	}
	node, exists := c.nodes[nodeID]
	if !exists {
		log.Printf("Error: Node %s not found for role change notification", nodeID)
		return nil
	}

	url := fmt.Sprintf("http://%s/partition-update", node.Address)
	data := map[string]interface{}{
		"action":       "change_role",
		"partition_id": partitionID,
		"role":         role,
	}

	err := c.networkClient.Post(url, data, nil)
	if err != nil {
		log.Printf("Error notifying node %s of role change: %v", nodeID, err)
	}
	return err
}

// notifyNodeAddPartition sends a add partition notification to a node
func (c *Controller) notifyNodeAddPartition(node *model.Node, partitionID int, role model.NodeRole) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, exists := c.partitions[partitionID]; !exists {
		log.Printf("Error: Partition %d not found for add partition notification", partitionID)
		return nil
	}

	url := fmt.Sprintf("http://%s/partition-update", node.Address)
	data := map[string]interface{}{
		"action":       "add",
		"partition_id": partitionID,
		"role":         role,
	}

	err := c.networkClient.Post(url, data, nil)
	if err != nil {
		log.Printf("Error notifying node %s of adding partition %d: %v", node.ID, partitionID, err)
	}
	return err
}

// notifyNodeRemovePartition sends a add partition notification to a node
func (c *Controller) notifyNodeRemovePartition(node *model.Node, partitionID int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, exists := c.partitions[partitionID]; !exists {
		log.Printf("Error: Partition %d not found for remove partition notification", partitionID)
		return nil
	}

	url := fmt.Sprintf("http://%s/partition-update", node.Address)
	data := map[string]interface{}{
		"action":       "remove",
		"partition_id": partitionID,
	}

	err := c.networkClient.Post(url, data, nil)
	if err != nil {
		log.Printf("Error notifying node %s of removing partition %d: %v", node.ID, partitionID, err)
	}
	return err
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

	if err := c.updateNodeHeartbeat(data.ID); err != nil {
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

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Convert to a slice for JSON response
	nodesList := make([]model.Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodesList = append(nodesList, *node)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodesList)
}

func (c *Controller) handlePartitionList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Convert to a slice for JSON response
	partitionsList := make([]model.Partition, 0, len(c.partitions))
	for _, partition := range c.partitions {
		partitionsList = append(partitionsList, *partition)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(partitionsList)
}

func (c *Controller) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

// Helper function to check if a string slice contains a string
func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func retryJob(tickMillisec, timeoutSec int, task func() error) (err error) {
	tick := time.Tick(time.Duration(tickMillisec) * time.Millisecond)
	timeout := time.After(time.Duration(timeoutSec) * time.Second)

	for {
		select {
		case <-timeout:
			if err == nil {
				err = fmt.Errorf("timeout exceeded")
			}
			return err
		case <-tick:
			if err = task(); err == nil {
				return nil
			}
		}
	}
}
