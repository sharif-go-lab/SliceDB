package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/sharif-go-lab/SliceDB/internal/model"
)

type EtcdRegistry struct {
	client *clientv3.Client
}

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

func (r *EtcdRegistry) UpdateNodeHeartbeat(ctx context.Context, leaseID clientv3.LeaseID) error {
	_, err := r.client.KeepAliveOnce(ctx, leaseID)
	return err
}

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

func (r *EtcdRegistry) SetPartition(ctx context.Context, p model.Partition) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("/partitions/%d", p.ID)
	_, err = r.client.Put(ctx, key, string(data))
	return err
}

func (r *EtcdRegistry) GetPartitions(ctx context.Context) ([]model.Partition, error) {
	resp, err := r.client.Get(ctx, "/partitions/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var partitions []model.Partition
	for _, kv := range resp.Kvs {
		var p model.Partition
		if err := json.Unmarshal(kv.Value, &p); err == nil {
			partitions = append(partitions, p)
		}
	}
	return partitions, nil
}

func (r *EtcdRegistry) SetControllerAddress(ctx context.Context, address string) error {
	_, err := r.client.Put(ctx, "/controller/address", address)
	return err
}

func (r *EtcdRegistry) GetControllerAddress(ctx context.Context) (string, error) {
	resp, err := r.client.Get(ctx, "/controller/address", clientv3.WithPrefix())
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("controller address not found")
	}
	return string(resp.Kvs[0].Value), nil
}
