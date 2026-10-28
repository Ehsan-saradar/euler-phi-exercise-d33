// Command kvnode runs one node of the replicated key-value store. All
// configuration comes from KV_* environment variables so the same image runs
// everywhere.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/raftnode"
	"github.com/Ehsan-saradar/euler-phi-exercise-d33/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kvnode:", err)
		os.Exit(1)
	}
}

func run() error {
	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(env("KV_LOG_LEVEL", "info"))); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	peers, err := parsePeers(env("KV_PEERS", ""))
	if err != nil {
		return err
	}
	cfg := server.Config{
		ID:        env("KV_NODE_ID", "node1"),
		Listen:    env("KV_LISTEN", "0.0.0.0:7000"),
		Advertise: env("KV_ADVERTISE", ""),
		Peers:     peers,
		Bootstrap: envBool("KV_BOOTSTRAP", len(peers) > 0),
		DataDir:   env("KV_DATA_DIR", "/data"),
		Raft: raftnode.Config{
			HeartbeatTimeout:   envDuration("KV_HEARTBEAT_TIMEOUT", 500*time.Millisecond),
			ElectionTimeout:    envDuration("KV_ELECTION_TIMEOUT", 1000*time.Millisecond),
			LeaderLeaseTimeout: envDuration("KV_LEADER_LEASE_TIMEOUT", 400*time.Millisecond),
			CommitTimeout:      envDuration("KV_COMMIT_TIMEOUT", 20*time.Millisecond),
			SnapshotThreshold:  envUint("KV_SNAPSHOT_THRESHOLD", 8192),
			SnapshotInterval:   envDuration("KV_SNAPSHOT_INTERVAL", 120*time.Second),
			TrailingLogs:       envUint("KV_TRAILING_LOGS", 10240),
			ApplyTimeout:       envDuration("KV_APPLY_TIMEOUT", 5*time.Second),
			ReadMode:           raftnode.ReadMode(env("KV_READ_MODE", string(raftnode.ReadModeReadIndex))),
		},
		Logger: logger,
	}
	cfg.API.LeaderWait = envDuration("KV_LEADER_WAIT", 2*time.Second)
	if cfg.Advertise == "" && cfg.Peers != nil {
		cfg.Advertise = cfg.Peers[cfg.ID]
	}

	srv, err := server.Start(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Stop(shutdownCtx)
}

// parsePeers reads "id=host:port,id=host:port".
func parsePeers(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	peers := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("KV_PEERS entry %q is not id=address", part)
		}
		peers[id] = addr
	}
	return peers, nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envDuration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envUint(key string, def uint64) uint64 {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	u, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return def
	}
	return u
}
