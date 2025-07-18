package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

// Registry wraps etcd client for cluster coordination.
type Registry struct {
	client *clientv3.Client
}

// NewRegistry connects to etcd endpoints.
func NewRegistry(endpoints []string) (*Registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Registry{client: cli}, nil
}

// Close closes the underlying etcd client.
func (r *Registry) Close() error {
	return r.client.Close()
}

// Client exposes the underlying etcd client.
func (r *Registry) Client() *clientv3.Client {
	return r.client
}

// RegisterNode registers a node with a TTL lease and keeps it alive.
func (r *Registry) RegisterNode(ctx context.Context, node model.Node, ttl int64) (clientv3.LeaseID, error) {
	lease, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return 0, err
	}
	data, _ := json.Marshal(node)
	if _, err := r.client.Put(ctx, fmt.Sprintf("nodes/%s", node.ID), string(data), clientv3.WithLease(lease.ID)); err != nil {
		return 0, err
	}
	ch, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return 0, err
	}
	go func() {
		for range ch {
		}
	}()
	return lease.ID, nil
}

// GetNodes returns all registered nodes.
func (r *Registry) GetNodes(ctx context.Context) ([]model.Node, error) {
	resp, err := r.client.Get(ctx, "nodes/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	nodes := make([]model.Node, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var n model.Node
		if err := json.Unmarshal(kv.Value, &n); err == nil {
			nodes = append(nodes, n)
		}
	}
	return nodes, nil
}

// PutPartition stores partition info.
func (r *Registry) PutPartition(ctx context.Context, p model.Partition) error {
	data, _ := json.Marshal(p)
	_, err := r.client.Put(ctx, fmt.Sprintf("partitions/%d", p.ID), string(data))
	return err
}

// GetPartitions loads all partitions from etcd.
func (r *Registry) GetPartitions(ctx context.Context) ([]model.Partition, error) {
	resp, err := r.client.Get(ctx, "partitions/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	parts := make([]model.Partition, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var p model.Partition
		if err := json.Unmarshal(kv.Value, &p); err == nil {
			parts = append(parts, p)
		}
	}
	return parts, nil
}
