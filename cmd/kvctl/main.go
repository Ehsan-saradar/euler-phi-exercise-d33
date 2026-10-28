// Command kvctl is a small CLI over the gRPC API for demos, scripts and the
// Docker health check.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Ehsan-saradar/euler-phi-exercise-d33/gen/kv/v1"
)

const usage = `usage: kvctl [-addr host:port] [-timeout 5s] <command> [args]

  put <key> <value>              write a key
  get [-stale] <key>             read a key (linearizable unless -stale)
  del <key>                      delete a key
  range [-stale] [-limit n] [-after key] <prefix>
  cas <key> <expected-version> <value>   expected-version 0 = create only
  status                         raft view of the node
  join <id> <address>            add a voter (forwarded to the leader)
  leave <id>                     remove a server
  partition [-clear] [-block a,b] [-unblock a,b]
  snapshot                       force a snapshot
  health                         gRPC health probe (exit 0 when serving)

Environment: KVCTL_ADDR overrides the default address (localhost:7001).
`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", envOr("KVCTL_ADDR", "localhost:7001"), "node address")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	kv, admin := kvv1.NewKVServiceClient(conn), kvv1.NewAdminServiceClient(conn)
	args := flag.Args()[1:]
	var resp proto.Message
	switch flag.Arg(0) {
	case "put":
		need(args, 2)
		resp, err = kv.Put(ctx, &kvv1.PutRequest{Key: []byte(args[0]), Value: []byte(args[1])})
	case "get":
		fs := flag.NewFlagSet("get", flag.ExitOnError)
		stale := fs.Bool("stale", false, "serve from the local replica without coordination")
		_ = fs.Parse(args)
		need(fs.Args(), 1)
		resp, err = kv.Get(ctx, &kvv1.GetRequest{Key: []byte(fs.Arg(0)), Consistency: consistency(*stale)})
	case "del":
		need(args, 1)
		resp, err = kv.Delete(ctx, &kvv1.DeleteRequest{Key: []byte(args[0])})
	case "range":
		fs := flag.NewFlagSet("range", flag.ExitOnError)
		stale := fs.Bool("stale", false, "serve from the local replica without coordination")
		limit := fs.Int64("limit", 0, "page size")
		after := fs.String("after", "", "cursor: return keys after this one")
		_ = fs.Parse(args)
		resp, err = kv.Range(ctx, &kvv1.RangeRequest{Prefix: []byte(fs.Arg(0)), StartAfter: []byte(*after), Limit: *limit, Consistency: consistency(*stale)})
	case "cas":
		need(args, 3)
		ver, perr := strconv.ParseUint(args[1], 10, 64)
		if perr != nil {
			return perr
		}
		resp, err = kv.CompareAndSwap(ctx, &kvv1.CompareAndSwapRequest{Key: []byte(args[0]), ExpectedVersion: ver, Value: []byte(args[2])})
	case "status":
		resp, err = admin.Status(ctx, &kvv1.StatusRequest{})
	case "join":
		need(args, 2)
		resp, err = admin.Join(ctx, &kvv1.JoinRequest{Id: args[0], Address: args[1]})
	case "leave":
		need(args, 1)
		resp, err = admin.Leave(ctx, &kvv1.LeaveRequest{Id: args[0]})
	case "partition":
		fs := flag.NewFlagSet("partition", flag.ExitOnError)
		clearAll := fs.Bool("clear", false, "remove all blocks first")
		block := fs.String("block", "", "comma-separated peer ids to block")
		unblock := fs.String("unblock", "", "comma-separated peer ids to unblock")
		_ = fs.Parse(args)
		resp, err = admin.Partition(ctx, &kvv1.PartitionRequest{Block: split(*block), Unblock: split(*unblock), Clear: *clearAll})
	case "snapshot":
		resp, err = admin.Snapshot(ctx, &kvv1.SnapshotRequest{})
	case "health":
		h, herr := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if herr != nil {
			return fmt.Errorf("health check: %w", herr)
		}
		if h.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			return fmt.Errorf("health status %s", h.GetStatus())
		}
		fmt.Println("SERVING")
		return nil
	default:
		flag.Usage()
		return fmt.Errorf("unknown command %q", flag.Arg(0))
	}
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

func consistency(stale bool) kvv1.Consistency {
	if stale {
		return kvv1.Consistency_CONSISTENCY_STALE
	}
	return kvv1.Consistency_CONSISTENCY_LINEARIZABLE
}

func split(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func need(args []string, n int) {
	if len(args) < n {
		flag.Usage()
		os.Exit(2)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
