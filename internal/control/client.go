package control

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/infamousity/distributed-cache/internal/controlpb"
	"github.com/infamousity/distributed-cache/internal/version"
)

type ClientOptions struct {
	SharedKey   string
	TLS         *tls.Config
	DialTimeout time.Duration
}

type Client struct {
	conn   *grpc.ClientConn
	client controlpb.ControlPlaneClient
}

type Entry struct {
	Value     []byte
	Version   version.Version
	Tombstone bool
}

func Dial(addr string, opts ClientOptions) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is required")
	}

	creds := insecure.NewCredentials()
	if opts.TLS != nil {
		creds = credentials.NewTLS(opts.TLS)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(UnaryClientAuth(opts.SharedKey)),
	}

	var (
		conn *grpc.ClientConn
		err  error
	)
	if opts.DialTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
		defer cancel()
		dialOpts = append(dialOpts, grpc.WithBlock())
		conn, err = grpc.DialContext(ctx, addr, dialOpts...)
	} else {
		conn, err = grpc.Dial(addr, dialOpts...)
	}
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, client: controlpb.NewControlPlaneClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Ping(ctx context.Context) (string, error) {
	resp, err := c.client.Ping(ctx, &controlpb.PingRequest{})
	if err != nil {
		return "", err
	}
	return resp.GetNodeName(), nil
}

func (c *Client) Fetch(ctx context.Context, key string) (Entry, bool, error) {
	resp, err := c.client.Fetch(ctx, &controlpb.FetchRequest{Key: key})
	if err != nil {
		return Entry{}, false, err
	}
	return Entry{
		Value:     resp.GetValue(),
		Version:   fromProtoVersion(resp.GetVersion()),
		Tombstone: resp.GetTombstone(),
	}, resp.GetFound(), nil
}

func (c *Client) Store(ctx context.Context, key string, value []byte, ttl time.Duration, wc WriteConcern) error {
	return c.StoreVersioned(ctx, key, value, ttl, version.Zero(), wc)
}

func (c *Client) StoreVersioned(ctx context.Context, key string, value []byte, ttl time.Duration, ver version.Version, wc WriteConcern) error {
	_, err := c.client.Store(ctx, &controlpb.StoreRequest{
		Key:          key,
		Value:        value,
		TtlMs:        ttlMilliseconds(ttl),
		WriteConcern: toProtoWriteConcern(wc),
		Version:      toProtoVersion(ver),
	})
	return err
}

func (c *Client) Delete(ctx context.Context, key string, wc WriteConcern) error {
	return c.DeleteVersioned(ctx, key, version.Zero(), wc)
}

func (c *Client) DeleteVersioned(ctx context.Context, key string, ver version.Version, wc WriteConcern) error {
	_, err := c.client.Delete(ctx, &controlpb.DeleteRequest{Key: key, WriteConcern: toProtoWriteConcern(wc), Version: toProtoVersion(ver)})
	return err
}

func ttlMilliseconds(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	ms := ttl.Milliseconds()
	if ms == 0 {
		return 1
	}
	return ms
}

func toProtoVersion(ver version.Version) *controlpb.Version {
	if ver.IsZero() {
		return nil
	}
	return &controlpb.Version{Physical: ver.Physical, Logical: ver.Logical, NodeId: ver.NodeID}
}

func fromProtoVersion(ver *controlpb.Version) version.Version {
	if ver == nil {
		return version.Zero()
	}
	return version.Version{Physical: ver.GetPhysical(), Logical: ver.GetLogical(), NodeID: ver.GetNodeId()}
}
