// Package server boots one node: a single TCP port multiplexed between raft
// traffic and gRPC (cmux), so a node has exactly one address that peers dial
// for replication and that clients and forwarding followers dial for the API.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/api"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
)

// Config describes one node.
type Config struct {
	ID        string
	Listen    string            // bind address, e.g. 0.0.0.0:7000 (ignored when Listener is set)
	Listener  net.Listener      // pre-bound listener (tests)
	Advertise string            // address peers dial; defaults to the listener address
	Peers     map[string]string // id -> advertise address
	Bootstrap bool
	DataDir   string

	Raft raftnode.Config // timing and snapshot knobs; identity fields are filled in here
	API  api.Config

	Logger *slog.Logger
}

// Server is a running node.
type Server struct {
	cfg  Config
	ln   net.Listener
	mux  cmux.CMux
	grpc *grpc.Server
	api  *api.Server
	node *raftnode.Node
	log  *slog.Logger
	done chan struct{}
}

// Start binds the port, starts raft and serves gRPC.
func Start(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	log := cfg.Logger.With("node", cfg.ID)

	ln := cfg.Listener
	if ln == nil {
		var err error
		if ln, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.Listen); err != nil {
			return nil, fmt.Errorf("server: listen %s: %w", cfg.Listen, err)
		}
	}
	if cfg.Advertise == "" {
		cfg.Advertise = ln.Addr().String()
	}

	mux := cmux.New(ln)
	grpcL := mux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	raftL := mux.Match(cmux.Any())

	rcfg := cfg.Raft
	rcfg.ID = cfg.ID
	rcfg.Listener = raftL
	rcfg.Advertise = cfg.Advertise
	rcfg.Peers = cfg.Peers
	rcfg.Bootstrap = cfg.Bootstrap
	rcfg.DataDir = cfg.DataDir
	rcfg.Logger = log
	node, err := raftnode.New(rcfg)
	if err != nil {
		_ = ln.Close()
		return nil, err
	}

	apiSrv := api.New(node, cfg.API)
	g := grpc.NewServer()
	apiSrv.Register(g)
	h := health.NewServer()
	h.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(g, h)
	reflection.Register(g)

	s := &Server{cfg: cfg, ln: ln, mux: mux, grpc: g, api: apiSrv, node: node, log: log, done: make(chan struct{})}
	go func() {
		err := g.Serve(grpcL)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, cmux.ErrListenerClosed) && !errors.Is(err, cmux.ErrServerClosed) {
			log.Error("grpc server stopped", "err", err)
		}
	}()
	go func() {
		defer close(s.done)
		if err := mux.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Debug("cmux stopped", "err", err)
		}
	}()
	log.Info("node started", "advertise", cfg.Advertise, "data_dir", cfg.DataDir, "bootstrap", cfg.Bootstrap)
	return s, nil
}

// Addr returns the advertised address.
func (s *Server) Addr() string { return s.cfg.Advertise }

// Node exposes the raft node.
func (s *Server) Node() *raftnode.Node { return s.node }

// Stop shuts everything down; it returns once raft has stopped.
func (s *Server) Stop(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-ctx.Done():
		s.grpc.Stop()
	case <-time.After(3 * time.Second):
		s.grpc.Stop()
	}
	s.api.Close()
	err := s.node.Shutdown()
	s.mux.Close()
	_ = s.ln.Close()
	select {
	case <-s.done:
	case <-time.After(time.Second):
	}
	s.log.Info("node stopped")
	return err
}
