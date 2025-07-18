package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

// EtcdRegistry wraps an etcd client and provides helper methods for
// membership and service discovery.
type EtcdRegistry struct {
	client *clientv3.Client
}

// NewEtcdRegistry creates a new registry backed by etcd.
func NewEtcdRegistry(endpoints []string) (*EtcdRegistry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdRegistry{client: cli}, nil
}

// RegisterNode registers a node under /nodes/<id> using a lease for TTL.
func (r *EtcdRegistry) RegisterNode(ctx context.Context, node model.Node, ttl int64) (clientv3.LeaseID, error) {
	lease, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return 0, err
	}
	data, err := json.Marshal(node)
	if err != nil {
		return 0, err
	}
	_, err = r.client.Put(ctx, "/nodes/"+node.ID, string(data), clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}
	ch, err := r.client.KeepAlive(ctx, lease.ID)
	if err == nil {
		go func() {
			for range ch {
				// consume keep alive channel
			}
		}()
	}
	return lease.ID, err
}

// UpdateNodeHeartbeat renews the lease for a node.
func (r *EtcdRegistry) UpdateNodeHeartbeat(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := r.client.KeepAliveOnce(ctx, leaseID)
	return err
}

// GetNodes returns all registered nodes.
func (r *EtcdRegistry) GetNodes(ctx context.Context) ([]model.Node, error) {
	resp, err := r.client.Get(ctx, "/nodes/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var nodes []model.Node
	for _, kv := range resp.Kvs {
		var n model.Node
		if err := json.Unmarshal(kv.Value, &n); err == nil {
			nodes = append(nodes, n)
		}
	}
	return nodes, nil
}

// SetPartition stores partition info under /partitions/<id>.
func (r *EtcdRegistry) SetPartition(ctx context.Context, p model.Partition) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("/partitions/%d", p.ID)
	_, err = r.client.Put(ctx, key, string(data))
	return err
}

// GetPartitions fetches all partitions from etcd.
func (r *EtcdRegistry) GetPartitions(ctx context.Context) ([]model.Partition, error) {
	resp, err := r.client.Get(ctx, "/partitions/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var parts []model.Partition
	for _, kv := range resp.Kvs {
		var p model.Partition
		if err := json.Unmarshal(kv.Value, &p); err == nil {
			parts = append(parts, p)
		}
	}
	return parts, nil
}
