// controlplane is the demo kill switch: whitelist stop/start of node containers.
//
//	CONTROL_NODES=1=kmc-node-1,2=kmc-node-2,3=kmc-node-3
//	HTTP_ADDR=0.0.0.0:8080
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/controlplane"
)

func main() {
	nodes, err := parseNodes(env("CONTROL_NODES", "1=kmc-node-1,2=kmc-node-2,3=kmc-node-3"))
	if err != nil {
		fatalf("%v", err)
	}
	eng, err := controlplane.NewEngine(controlplane.Config{
		Nodes:             nodes,
		GlobalKillsPerSec: 1.5,
		IPCooldown:        2 * time.Second,
	})
	if err != nil {
		fatalf("%v", err)
	}
	defer eng.Close()

	addr := env("HTTP_ADDR", "0.0.0.0:8080")
	fmt.Printf("whitelist: %v\n", nodes)
	if err := controlplane.ListenAndServe(addr, eng); err != nil {
		fatalf("%v", err)
	}
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
