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
	ID              string
	Address         string
	ControllerAddr  string
	partitions      map[int]*partition.Partition
	partitionCount  int
	mu              sync.RWMutex
	networkClient   *network.Client
	heartbeatTicker *time.Ticker
}

// NewNode creates a new database node
func NewNode(id, address, controllerAddr string) *Node {
	node := &Node{
		ID:             id,
		Address:        address,
		ControllerAddr: controllerAddr,
		partitions:     make(map[int]*partition.Partition),
		partitionCount: 10, // Default partition count
		networkClient:  network.NewClient(),
	}

	return node
}

// Start begins the node operation
func (n *Node) Start() error {
	// Register with controller
	err := n.registerWithController()
	if err != nil {
		return fmt.Errorf("failed to register with controller: %v", err)
	}

	// Start heartbeat
	n.startHeartbeat()

	// Start HTTP server for node communication
	http.HandleFunc("/set", n.handleSet)
	http.HandleFunc("/get", n.handleGet)
	http.HandleFunc("/delete", n.handleDelete)
	http.HandleFunc("/apply-log", n.handleApplyLog)
	http.HandleFunc("/partition-update", n.handlePartitionUpdate)
	http.HandleFunc("/health", n.handleHealth)

	log.Printf("Node %s starting on %s", n.ID, n.Address)
	return http.ListenAndServe(n.Address, nil)
}

// registerWithController registers this node with the controller
func (n *Node) registerWithController() error {
	registerURL := fmt.Sprintf("http://%s/register", n.ControllerAddr)

	data := map[string]string{
		"id":      n.ID,
		"address": n.Address,
	}

	var response map[string]interface{}
	err := n.networkClient.Post(registerURL, data, &response)

	return err
}

// startHeartbeat begins sending periodic heartbeats to the controller
func (n *Node) startHeartbeat() {
	n.heartbeatTicker = time.NewTicker(5 * time.Second)

	go func() {
		for range n.heartbeatTicker.C {
			n.sendHeartbeat()
		}
	}()
}

// sendHeartbeat sends a heartbeat to the controller
func (n *Node) sendHeartbeat() {
	heartbeatURL := fmt.Sprintf("http://%s/heartbeat", n.ControllerAddr)

	data := map[string]string{
		"id":     n.ID,
		"status": "healthy",
	}

	err := n.networkClient.Post(heartbeatURL, data, nil)
	if err != nil {
		log.Printf("Error sending heartbeat: %v", err)
	}
}

// AddPartition adds a partition to this node
func (n *Node) AddPartition(partitionID int, role model.NodeRole) {
	n.mu.Lock()
	defer n.mu.Unlock()

	p := partition.NewPartition(partitionID, role)
	n.partitions[partitionID] = p

	log.Printf("Node %s added partition %d with role %s", n.ID, partitionID, role)
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

	return p.Set(key, value)
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

	return p.Delete(key)
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

	p.ApplyLogEntry(entry)
	w.WriteHeader(http.StatusOK)
}

func (n *Node) handlePartitionUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data struct {
		Action      string         `json:"action"` // "add" or "remove" or "change_role"
		PartitionID int            `json:"partition_id"`
		Role        model.NodeRole `json:"role,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch data.Action {
	case "add":
		n.AddPartition(data.PartitionID, data.Role)
	case "remove":
		n.RemovePartition(data.PartitionID)
	case "change_role":
		n.mu.RLock()
		p, exists := n.partitions[data.PartitionID]
		n.mu.RUnlock()

		if !exists {
			http.Error(w, "Partition not found", http.StatusNotFound)
			return
		}

		p.ChangeRole(data.Role)
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
