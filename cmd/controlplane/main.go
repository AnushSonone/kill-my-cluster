// controlplane is the demo kill switch: whitelist stop/start/partition + auto-heal.
//
//	CONTROL_NODES=1=kmc-node-1,2=kmc-node-2,...
//	CONTROL_NETWORK=kmc_kmc
//	HEAL_AFTER=10s
//	HTTP_ADDR=0.0.0.0:8080
//	PROMETHEUS_URL=http://prometheus:9090
//	ALLOW_RESET=false
//	CORS_ORIGINS=https://anush.wiki,http://127.0.0.1:3000
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/controlplane"
)

func main() {
	nodes, err := parseNodes(env("CONTROL_NODES", defaultNodes()))
	if err != nil {
		fatalf("%v", err)
	}
	healAfter, err := time.ParseDuration(env("HEAL_AFTER", "10s"))
	if err != nil {
		fatalf("HEAL_AFTER: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rates := controlplane.NewRateCache(env("PROMETHEUS_URL", ""))
	go rates.Run(ctx)

	eng, err := controlplane.NewEngine(controlplane.Config{
		Nodes:             nodes,
		Network:           env("CONTROL_NETWORK", "kmc_kmc"),
		GlobalKillsPerSec: 1.5,
		IPCooldown:        2 * time.Second,
		HealAfter:         healAfter,
		Rates:             rates,
	})
	if err != nil {
		fatalf("%v", err)
	}
	defer eng.Close()

	addr := env("HTTP_ADDR", "0.0.0.0:8080")
	allowReset := strings.EqualFold(env("ALLOW_RESET", "false"), "true")
	fmt.Printf("whitelist: %d nodes · network=%s · heal=%s · reset=%v\n",
		len(nodes), env("CONTROL_NETWORK", "kmc_kmc"), healAfter, allowReset)
	if err := controlplane.ListenAndServe(addr, eng, controlplane.ServerOptions{
		AllowReset:  allowReset,
		CORSOrigins: env("CORS_ORIGINS", ""),
	}); err != nil {
		fatalf("%v", err)
	}
}

func defaultNodes() string {
	parts := make([]string, 0, 7)
	for i := 1; i <= 7; i++ {
		parts = append(parts, fmt.Sprintf("%d=kmc-node-%d", i, i))
	}
	return strings.Join(parts, ",")
}

func parseNodes(s string) ([]controlplane.Node, error) {
	var out []controlplane.Node
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, name, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("CONTROL_NODES: want id=container, got %q", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, controlplane.Node{ID: id, ContainerName: strings.TrimSpace(name)})
	}
	return out, nil
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
