// node is a single Raft + KV process for Docker (and bare-metal) deployments.
//
// Environment:
//
//	NODE_ID=1
//	DATA_DIR=/data
//	RAFT_ADDR=0.0.0.0:7000
//	KV_ADDR=0.0.0.0:8000
//	METRICS_ADDR=0.0.0.0:9100
//	PEERS=2=node2:7000,3=node3:7000          # other Raft peers (id=host:port)
//	KV_PEERS=1=node1:8000,2=node2:8000,...   # all KV endpoints (for traffic agent)
//	RUN_AGENT=true                           # only one node should set this
//	SNAPSHOT_ENTRIES=10000                   # auto-compact every N applies
//	SNAPSHOT_RETAIN=1024                     # entries kept behind the snapshot;
//	                                         # raising it costs throughput, see
//	                                         # SnapshotRetain in internal/raft
//	RAFT_COMMIT_STALL_TIMEOUT=30s            # leader self-deposes after this
//	                                         # long with no commit progress
//	                                         # (negative disables the watchdog)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
	"github.com/AnushSonone/kill-my-cluster/internal/metrics"
	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fatalf("%v", err)
	}
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		fatalf("data dir: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transport := raft.NewGRPCTransport(cfg.raftPeers)
	cl, err := kv.NewCluster(kv.Config{
		ID: cfg.id, Peers: cfg.peerIDs, Dir: cfg.dataDir,
		Transport:       transport,
		Timings:         cfg.timings,
		SnapshotEntries: cfg.snapshotEntries,
		SnapshotRetain:  cfg.snapshotRetain,
	})
	if err != nil {
		fatalf("cluster: %v", err)
	}

	col := metrics.NewCollector(cfg.id)
	cl.SetTelemetry(col)
	go metrics.NewReporter(cl.Raft(), col, 250*time.Millisecond).Run(ctx)

	raftSrv, err := raft.NewServer(cl.Raft(), cfg.raftAddr)
	if err != nil {
		fatalf("raft server: %v", err)
	}
	kvSrv, err := kv.NewKVServer(cl, cfg.kvAddr)
	if err != nil {
		fatalf("kv server: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", col.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		rn := cl.Raft()
		term, isLeader := rn.Status()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"id":          cfg.id,
			"term":        term,
			"commitIndex": rn.CommitIndex(),
			"leaderId":    rn.LeaderID(),
			"isLeader":    isLeader,
			"role":        rn.Role().String(),
		})
	})
	httpSrv := &http.Server{Addr: cfg.metricsAddr, Handler: mux}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "metrics: %v\n", err)
		}
	}()

	fmt.Printf("node %d up\n", cfg.id)
	fmt.Printf("  raft    %s\n", raftSrv.Addr())
	fmt.Printf("  kv      %s\n", kvSrv.Addr())
	fmt.Printf("  metrics http://%s/metrics\n", cfg.metricsAddr)

	var kvClient *kv.Client
	if cfg.runAgent {
		kvClient = kv.NewClient(cfg.kvPeers)
		go runTraffic(ctx, kvClient)
		fmt.Println("  traffic agent starting (waits for quorum)…")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down…")
	cancel()
	if kvClient != nil {
		kvClient.Close()
	}
	_ = httpSrv.Close()
	kvSrv.Stop()
	raftSrv.Stop()
	cl.Stop()
	transport.Close()
}

type config struct {
	id          uint64
	dataDir     string
	raftAddr    string
	kvAddr      string
	metricsAddr string
	peerIDs     []uint64
	raftPeers   map[uint64]string
	kvPeers     map[uint64]string
	runAgent        bool
	timings         raft.Timings
	snapshotEntries uint64
	snapshotRetain  int
}

func loadConfig() (config, error) {
	id, err := strconv.ParseUint(env("NODE_ID", ""), 10, 64)
	if err != nil || id == 0 {
		return config{}, fmt.Errorf("NODE_ID must be a positive integer")
	}
	raftPeers, err := parsePeers(env("PEERS", ""))
	if err != nil {
		return config{}, fmt.Errorf("PEERS: %w", err)
	}
	peerIDs := make([]uint64, 0, len(raftPeers))
	for pid := range raftPeers {
		peerIDs = append(peerIDs, pid)
	}
	kvPeers, err := parsePeers(env("KV_PEERS", ""))
	if err != nil {
		return config{}, fmt.Errorf("KV_PEERS: %w", err)
	}
	runAgent := strings.EqualFold(env("RUN_AGENT", "false"), "true")
	if runAgent && len(kvPeers) == 0 {
		return config{}, fmt.Errorf("RUN_AGENT=true requires KV_PEERS")
	}
	return config{
		id:          id,
		dataDir:     env("DATA_DIR", "/data"),
		raftAddr:    env("RAFT_ADDR", "0.0.0.0:7000"),
		kvAddr:      env("KV_ADDR", "0.0.0.0:8000"),
		metricsAddr: env("METRICS_ADDR", "0.0.0.0:9100"),
		peerIDs:     peerIDs,
		raftPeers:   raftPeers,
		kvPeers:     kvPeers,
		runAgent:        runAgent,
		timings:         loadTimings(),
		snapshotEntries: loadSnapshotEntries(),
		snapshotRetain:  loadSnapshotRetain(),
	}, nil
}

// loadSnapshotRetain reads SNAPSHOT_RETAIN (0/unset = raft default of 1024).
// Raising it is measurably expensive on a CPU-capped box — see the cost table
// on SnapshotRetain in internal/raft before changing it. Negative is passed
// through: it means "keep nothing", which tests rely on.
func loadSnapshotRetain() int {
	v := os.Getenv("SNAPSHOT_RETAIN")
	if v == "" {
		return 0
	}
	nv, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ignoring SNAPSHOT_RETAIN=%q: %v\n", v, err)
		return 0
	}
	return nv
}

// loadSnapshotEntries reads SNAPSHOT_ENTRIES (0/unset = kv default of 10000).
// Floored at 512 in production: more frequent snapshots than that spend more
// time rewriting the WAL than the compaction saves.
func loadSnapshotEntries() uint64 {
	v := os.Getenv("SNAPSHOT_ENTRIES")
	if v == "" {
		return 0
	}
	nv, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ignoring SNAPSHOT_ENTRIES=%q: %v\n", v, err)
		return 0
	}
	if nv < 512 {
		nv = 512
	}
	return nv
}

// loadTimings reads optional Raft clock overrides. Unset or malformed values
// fall back to raft defaults (zero fields), so a bad env never bricks a node.
func loadTimings() raft.Timings {
	dur := func(key string) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return 0
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			fmt.Fprintf(os.Stderr, "ignoring %s=%q: %v\n", key, v, err)
			return 0
		}
		return d
	}
	// The commit-stall watchdog is the one clock where a negative value is
	// meaningful ("disable"), so it cannot use dur, which floors at 0.
	stall := func(key string) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return 0
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ignoring %s=%q: %v\n", key, v, err)
			return 0
		}
		return d
	}
	return raft.Timings{
		ElectionTimeoutMin: dur("RAFT_ELECTION_TIMEOUT_MIN"),
		ElectionTimeoutMax: dur("RAFT_ELECTION_TIMEOUT_MAX"),
		HeartbeatInterval:  dur("RAFT_HEARTBEAT_INTERVAL"),
		TickInterval:       dur("RAFT_TICK_INTERVAL"),
		CommitStallTimeout: stall("RAFT_COMMIT_STALL_TIMEOUT"),
	}
}

func parsePeers(s string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("want id=host:port, got %q", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, err
		}
		out[id] = strings.TrimSpace(addr)
	}
	return out, nil
}

// runTraffic writes a heartbeat key on an interval so commit indexes / applies
// keep moving for the public proof (Grafana + mesh pulse).
func runTraffic(ctx context.Context, client *kv.Client) {
	interval := agentInterval()
	var seq uint64
	fmt.Printf("  traffic agent running (interval %s)\n", interval)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			seq++
			val := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
			_, err := client.ExecuteOnce(ctx, "traffic", seq, kv.Command{
				Op: kv.OpPut, Key: "demo/heartbeat", Value: val,
			})
			if err != nil {
				// No quorum yet — reuse the same request id on the next tick.
				seq--
			}
		}
	}
}

func agentInterval() time.Duration {
	if v := os.Getenv("AGENT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 1500 * time.Millisecond
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
