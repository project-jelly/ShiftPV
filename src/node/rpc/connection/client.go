package connection

import (
	"context"
	"fmt"
	"sync"

	"github.com/project-jelly/ShiftPV/src/kubernetes/volumeapi"
	"github.com/project-jelly/ShiftPV/src/node/rpc/protocol"
	"github.com/project-jelly/ShiftPV/src/node/rpc/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Target struct {
	Identity             volumeapi.NodeExecutor
	Address, Certificate string
}
type cached struct {
	target Target
	conn   *grpc.ClientConn
}

// Client keeps one connection per node incarnation. A changed address or
// process certificate replaces it; no Service balances effects across nodes.
type Client struct {
	TokenFile   string
	mu          sync.Mutex
	connections map[string]cached
	closed      bool
}

func (c *Client) client(target Target) (protocol.NodeClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("Node RPC client is closed")
	}
	if !target.Identity.Valid() || target.Address == "" || c.TokenFile == "" {
		return nil, fmt.Errorf("incomplete Node RPC target")
	}
	if old, ok := c.connections[target.Identity.NodeName]; ok {
		if old.target == target {
			return protocol.NewNodeClient(old.conn), nil
		}
		_ = old.conn.Close()
		delete(c.connections, target.Identity.NodeName)
	}
	tlsConfig, err := security.ClientTLS(target.Identity.PodUID, target.Certificate)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient("passthrough:///"+target.Address, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithPerRPCCredentials(security.FileToken{Path: c.TokenFile}), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64*1024), grpc.MaxCallSendMsgSize(64*1024)))
	if err != nil {
		return nil, err
	}
	if c.connections == nil {
		c.connections = map[string]cached{}
	}
	c.connections[target.Identity.NodeName] = cached{target: target, conn: conn}
	return protocol.NewNodeClient(conn), nil
}
func (c *Client) Execute(ctx context.Context, target Target, operation protocol.Operation, req *protocol.EffectRequest) error {
	client, err := c.client(target)
	if err != nil {
		return err
	}
	switch operation {
	case protocol.Operation_CREATE:
		_, err = client.CreateCopy(ctx, req)
	case protocol.Operation_RECLAIM:
		_, err = client.ReclaimCopy(ctx, req)
	default:
		return fmt.Errorf("unsupported Node operation")
	}
	return err
}
func (c *Client) GetCapacity(ctx context.Context, target Target, req *protocol.CapacityRequest) (*protocol.CapacityResponse, error) {
	client, err := c.client(target)
	if err != nil {
		return nil, err
	}
	return client.GetCapacity(ctx, req)
}
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for key, entry := range c.connections {
		_ = entry.conn.Close()
		delete(c.connections, key)
	}
	return nil
}
