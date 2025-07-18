package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/sharif-go-lab/SliceDB/internal/model"
	"github.com/sharif-go-lab/SliceDB/internal/registry"
	"github.com/sharif-go-lab/SliceDB/pkg/hash"
	"github.com/sharif-go-lab/SliceDB/pkg/network"
)

// Controller manages the cluster nodes and partitions
type Controller struct {
	partitionCount    int
	replicationFactor int
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
		registry:          reg,
		partitionCount:    partitionCount,
		replicationFactor: replicationFactor,
		networkClient:     network.NewClient(),
	}
}

// Start begins the controller operation
func (c *Controller) Start(addr string) error {
	// Initialize partitions from etcd if possible
	if err := c.initializePartitions(); err != nil {
		return err
	}

	// Start heartbeat checker
	go c.startBalancer()

	// Set up HTTP handlers
	http.HandleFunc("/set", c.handleSet)
	http.HandleFunc("/get", c.handleGet)
	http.HandleFunc("/delete", c.handleDelete)
	http.HandleFunc("/health", c.handleHealth)
	http.HandleFunc("/node-list", c.handleNodeList)
	http.HandleFunc("/partition-list", c.handlePartitionList)

	log.Printf("Controller starting on %s", addr)
	return http.ListenAndServe(addr, nil)
}

// initializePartitions creates the initial partition map without assigning nodes
func (c *Controller) initializePartitions() error {
	partitions, err := c.registry.GetPartitions(context.Background())
	if err == nil && len(partitions) == c.partitionCount {
		return nil
	}

	for i := 0; i < c.partitionCount; i++ {
		partition := model.Partition{
			ID:          i,
			LeaderID:    "",
			FollowerIDs: make([]string, 0),
		}
		if err := c.registry.SetPartition(context.Background(), partition); err != nil {
			log.Printf("failed to set partition %d: %v", i, err)
		}
	}
	return nil
}

// startRebalance periodically rebalance the partitions
func (c *Controller) startBalancer() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		c.rebalancePartitions()
	}
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
		if !model.ContainsString(healthyNodes, partition.LeaderID) {
			partition.LeaderID = ""
		}
		healthyFollowers := make([]string, 0)
		for _, nodeID := range partition.FollowerIDs {
			if model.ContainsString(healthyNodes, nodeID) {
				healthyFollowers = append(healthyFollowers, nodeID)
			}
		}
		partition.FollowerIDs = healthyFollowers

		// Ensure we have enough followers
		desiredFollowers := min(c.replicationFactor, len(healthyNodes))
		currentFollowers := len(partition.FollowerIDs)
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
				partition.FollowerIDs = append(partition.FollowerIDs, node.ID)
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
				} else {
					log.Printf("Rebalance: Removed follower %s from partition %d", followerID, partition.ID)
				}
			}
			partition.FollowerIDs = partition.FollowerIDs[:desiredFollowers]
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

			partition.LeaderID = newLeaderID
			partition.FollowerIDs = updatedFollowers
			if err := c.notifyNodeBecomingLeader(newLeaderID, partition.ID); err != nil {
				return
			}
			log.Printf("Rebalance: Assigned leader for partition %d to node %s", partition.ID, newLeaderID)
		}

		if err := c.registry.SetPartition(context.Background(), partition); err != nil {
			log.Printf("failed to set partition %d: %v", partition.ID, err)
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

	url := fmt.Sprintf("http://%s/partition-update", node.ID+node.Address)
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

	url := fmt.Sprintf("http://%s/partition-update", node.ID+node.Address)
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

	url := fmt.Sprintf("http://%s/partition-update", node.ID+node.Address)
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

func (c *Controller) handleNodeList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(c.getNodes()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (c *Controller) handlePartitionList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(c.getPartitions()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (c *Controller) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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

	url := fmt.Sprintf("http://%s/set", leaderNode.ID+leaderNode.Address)

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
	url := fmt.Sprintf("http://%s/get?key=%s", leaderNode.ID+leaderNode.Address, key)

	if err := c.networkClient.Get(url, &response); err != nil {
		if err.Error() == "received non-200 response: 404" {
			http.Error(w, "Key not found", http.StatusNotFound)
		} else {
			http.Error(w, fmt.Sprintf("Failed to forward request to leader: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to forward request to leader: %v", err), http.StatusInternalServerError)
	}
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

	url := fmt.Sprintf("http://%s/delete", leaderNode.ID+leaderNode.Address)

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
