package control

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/infamousity/distributed-cache/internal/controlpb"
	"github.com/infamousity/distributed-cache/internal/log"
	"github.com/infamousity/distributed-cache/internal/version"
)

type Handler interface {
	NodeName() string
	Fetch(ctx context.Context, key string) (Entry, bool, error)
	Store(ctx context.Context, key string, value []byte, ttl time.Duration, version version.Version, wc WriteConcern) error
	Delete(ctx context.Context, key string, version version.Version, wc WriteConcern) error
}

type ServerOptions struct {
	BindAddr  string
	SharedKey string
	TLS       *tls.Config
}

type Server struct {
	controlpb.UnimplementedControlPlaneServer

	handler Handler
	logger  log.Interface
	srv     *grpc.Server
	lis     net.Listener
	health  *health.Server
}

func NewServer(handler Handler, opts ServerOptions) (*Server, error) {
	if handler == nil {
		return nil, fmt.Errorf("handler is nil")
	}
	if opts.BindAddr == "" {
		return nil, fmt.Errorf("bind address is required")
	}

	lis, err := net.Listen("tcp", opts.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", opts.BindAddr, err)
	}

	serverOpts := []grpc.ServerOption{
		grpc.UnaryInterceptor(UnaryServerAuth(opts.SharedKey)),
	}
	if opts.TLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}

	srv := grpc.NewServer(serverOpts...)
	cs := &Server{
		handler: handler,
		logger:  log.Default(),
		srv:     srv,
		lis:     lis,
		health:  health.NewServer(),
	}
	controlpb.RegisterControlPlaneServer(srv, cs)
	grpc_health_v1.RegisterHealthServer(srv, cs.health)
	return cs, nil
}

func (s *Server) Start() {
	s.health.SetServingStatus("control.ControlPlane", grpc_health_v1.HealthCheckResponse_SERVING)
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	go func() {
		if err := s.srv.Serve(s.lis); err != nil {
			s.logger.Errorf("control-plane serve error: %v", err)
		}
	}()
}

func (s *Server) Stop() {
	s.health.SetServingStatus("control.ControlPlane", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	s.srv.GracefulStop()
}

func (s *Server) Addr() string {
	if s == nil || s.lis == nil {
		return ""
	}
	return s.lis.Addr().String()
}

func (s *Server) Ping(ctx context.Context, _ *controlpb.PingRequest) (*controlpb.PingResponse, error) {
	return &controlpb.PingResponse{NodeName: s.handler.NodeName()}, nil
}

func (s *Server) Fetch(ctx context.Context, req *controlpb.FetchRequest) (*controlpb.FetchResponse, error) {
	entry, found, err := s.handler.Fetch(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	return &controlpb.FetchResponse{
		Found:     found,
		Value:     entry.Value,
		Version:   toProtoVersion(entry.Version),
		Tombstone: entry.Tombstone,
	}, nil
}

func (s *Server) Store(ctx context.Context, req *controlpb.StoreRequest) (*controlpb.StoreResponse, error) {
	ttl, err := durationFromMilliseconds(req.GetTtlMs())
	if err != nil {
		return nil, err
	}
	if err := s.handler.Store(ctx, req.GetKey(), req.GetValue(), ttl, fromProtoVersion(req.GetVersion()), fromProtoWriteConcern(req.GetWriteConcern())); err != nil {
		return nil, rpcError(err)
	}
	return &controlpb.StoreResponse{Ok: true}, nil
}

func durationFromMilliseconds(milliseconds int64) (time.Duration, error) {
	const maxDurationMilliseconds = int64((1<<63 - 1) / int64(time.Millisecond))
	if milliseconds < 0 || milliseconds > maxDurationMilliseconds {
		return 0, status.Errorf(codes.InvalidArgument, "ttl_ms must be between 0 and %d", maxDurationMilliseconds)
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

func (s *Server) Delete(ctx context.Context, req *controlpb.DeleteRequest) (*controlpb.DeleteResponse, error) {
	if err := s.handler.Delete(ctx, req.GetKey(), fromProtoVersion(req.GetVersion()), fromProtoWriteConcern(req.GetWriteConcern())); err != nil {
		return nil, rpcError(err)
	}
	return &controlpb.DeleteResponse{Ok: true}, nil
}

type versionConflict interface {
	CurrentVersion() version.Version
}

func rpcError(err error) error {
	var conflict versionConflict
	if !errors.As(err, &conflict) {
		return err
	}
	current := conflict.CurrentVersion()
	st := status.New(codes.Aborted, fmt.Sprintf("stale cache version; current version is %s", current))
	detail := toProtoVersion(current)
	if detail == nil {
		return st.Err()
	}
	withDetails, detailsErr := st.WithDetails(detail)
	if detailsErr != nil {
		return st.Err()
	}
	return withDetails.Err()
}
