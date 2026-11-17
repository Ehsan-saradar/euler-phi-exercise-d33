// Command kvctl is a small CLI over the gRPC API for demos, scripts and the
// Docker health check.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}

// ctl holds the connection settings shared by every subcommand through the
// root command's persistent flags.
type ctl struct {
	addr    string
	timeout time.Duration
}

// rpc is one request against the data or admin service.
type rpc func(ctx context.Context, kv kvv1.KVServiceClient, admin kvv1.AdminServiceClient) (proto.Message, error)

// call dials the node, runs one RPC under the request timeout and prints the
// response as JSON. Every subcommand except health goes through it.
func (c *ctl) call(fn rpc) error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	resp, err := fn(ctx, kvv1.NewKVServiceClient(conn), kvv1.NewAdminServiceClient(conn))
	if err != nil {
		return err
	}
	out, err := protojson.MarshalOptions{Multiline: true, EmitUnpopulated: true}.Marshal(resp)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

func (c *ctl) dial() (*grpc.ClientConn, error) {
	return grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func newRootCmd() *cobra.Command {
	c := &ctl{}
	root := &cobra.Command{
		Use:   "kvctl",
		Short: "CLI for the replicated key-value store",
		Long: `kvctl talks gRPC to one node. Writes and linearizable reads sent to a
follower are forwarded to the leader, so any node address works.

Environment: KVCTL_ADDR overrides the default address (localhost:7001).`,
		// Argument errors are caught before PersistentPreRun and still print the
		// usage; once a command actually runs, an RPC error should not.
		PersistentPreRun: func(cmd *cobra.Command, _ []string) { cmd.SilenceUsage = true },
		SilenceErrors:    true, // main prints the error once, prefixed with the program name
	}
	root.PersistentFlags().StringVar(&c.addr, "addr", envOr("KVCTL_ADDR", "localhost:7001"), "node address")
	root.PersistentFlags().DurationVar(&c.timeout, "timeout", 5*time.Second, "request timeout")
	root.AddCommand(
		putCmd(c), getCmd(c), delCmd(c), rangeCmd(c), casCmd(c),
		statusCmd(c), joinCmd(c), leaveCmd(c), partitionCmd(c), snapshotCmd(c), healthCmd(c),
	)
	return root
}

func putCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "put <key> <value>",
		Short: "write a key",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.call(func(ctx context.Context, kv kvv1.KVServiceClient, _ kvv1.AdminServiceClient) (proto.Message, error) {
				return kv.Put(ctx, &kvv1.PutRequest{Key: []byte(args[0]), Value: []byte(args[1])})
			})
		},
	}
}

func getCmd(c *ctl) *cobra.Command {
	var stale bool
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "read a key (linearizable unless --stale)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.call(func(ctx context.Context, kv kvv1.KVServiceClient, _ kvv1.AdminServiceClient) (proto.Message, error) {
				return kv.Get(ctx, &kvv1.GetRequest{Key: []byte(args[0]), Consistency: consistency(stale)})
			})
		},
	}
	cmd.Flags().BoolVar(&stale, "stale", false, "serve from the local replica without coordination")
	return cmd
}

func delCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "del <key>",
		Short: "delete a key",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.call(func(ctx context.Context, kv kvv1.KVServiceClient, _ kvv1.AdminServiceClient) (proto.Message, error) {
				return kv.Delete(ctx, &kvv1.DeleteRequest{Key: []byte(args[0])})
			})
		},
	}
}

func rangeCmd(c *ctl) *cobra.Command {
	var (
		stale bool
		limit int64
		after string
	)
	cmd := &cobra.Command{
		Use:   "range [prefix]",
		Short: "list keys under a prefix, paged with --limit and --after",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			var prefix string
			if len(args) == 1 {
				prefix = args[0]
			}
			return c.call(func(ctx context.Context, kv kvv1.KVServiceClient, _ kvv1.AdminServiceClient) (proto.Message, error) {
				return kv.Range(ctx, &kvv1.RangeRequest{
					Prefix: []byte(prefix), StartAfter: []byte(after), Limit: limit, Consistency: consistency(stale),
				})
			})
		},
	}
	cmd.Flags().BoolVar(&stale, "stale", false, "serve from the local replica without coordination")
	cmd.Flags().Int64Var(&limit, "limit", 0, "page size (0 = server default)")
	cmd.Flags().StringVar(&after, "after", "", "cursor: return keys after this one")
	return cmd
}

func casCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "cas <key> <expected-version> <value>",
		Short: "compare-and-swap; expected-version 0 means create only",
		Args:  cobra.ExactArgs(3),
		RunE: func(_ *cobra.Command, args []string) error {
			version, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("expected-version: %w", err)
			}
			return c.call(func(ctx context.Context, kv kvv1.KVServiceClient, _ kvv1.AdminServiceClient) (proto.Message, error) {
				return kv.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{Key: []byte(args[0]), ExpectedVersion: version, Value: []byte(args[2])})
			})
		},
	}
}

func statusCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "raft view of the node",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return c.call(func(ctx context.Context, _ kvv1.KVServiceClient, admin kvv1.AdminServiceClient) (proto.Message, error) {
				return admin.Status(ctx, &kvv1.StatusRequest{})
			})
		},
	}
}

func joinCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "join <id> <address>",
		Short: "add a voter (forwarded to the leader)",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.call(func(ctx context.Context, _ kvv1.KVServiceClient, admin kvv1.AdminServiceClient) (proto.Message, error) {
				return admin.Join(ctx, &kvv1.JoinRequest{Id: args[0], Address: args[1]})
			})
		},
	}
}

func leaveCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "leave <id>",
		Short: "remove a server",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return c.call(func(ctx context.Context, _ kvv1.KVServiceClient, admin kvv1.AdminServiceClient) (proto.Message, error) {
				return admin.Leave(ctx, &kvv1.LeaveRequest{Id: args[0]})
			})
		},
	}
}

func partitionCmd(c *ctl) *cobra.Command {
	var (
		clearAll       bool
		block, unblock []string
	)
	cmd := &cobra.Command{
		Use:   "partition",
		Short: "block or unblock raft traffic to peers (fault injection)",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return c.call(func(ctx context.Context, _ kvv1.KVServiceClient, admin kvv1.AdminServiceClient) (proto.Message, error) {
				return admin.Partition(ctx, &kvv1.PartitionRequest{Block: block, Unblock: unblock, Clear: clearAll})
			})
		},
	}
	cmd.Flags().BoolVar(&clearAll, "clear", false, "remove all blocks first")
	cmd.Flags().StringSliceVar(&block, "block", nil, "comma-separated peer ids to block")
	cmd.Flags().StringSliceVar(&unblock, "unblock", nil, "comma-separated peer ids to unblock")
	return cmd
}

func snapshotCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot",
		Short: "force a snapshot",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return c.call(func(ctx context.Context, _ kvv1.KVServiceClient, admin kvv1.AdminServiceClient) (proto.Message, error) {
				return admin.Snapshot(ctx, &kvv1.SnapshotRequest{})
			})
		},
	}
}

func healthCmd(c *ctl) *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "gRPC health probe (exit 0 when serving)",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			conn, err := c.dial()
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
			defer cancel()
			h, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			if err != nil {
				return fmt.Errorf("health check: %w", err)
			}
			if h.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
				return fmt.Errorf("health status %s", h.GetStatus())
			}
			fmt.Println("SERVING")
			return nil
		},
	}
}

func consistency(stale bool) kvv1.Consistency {
	if stale {
		return kvv1.Consistency_CONSISTENCY_STALE
	}
	return kvv1.Consistency_CONSISTENCY_LINEARIZABLE
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
