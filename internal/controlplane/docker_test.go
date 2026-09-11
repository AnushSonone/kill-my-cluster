package controlplane

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDocker serves h on a unix socket, the way the real daemon does. The
// directory comes from os.MkdirTemp rather than t.TempDir because macOS caps
// unix socket paths at 104 bytes and test names make t.TempDir paths long.
func fakeDocker(t *testing.T, h http.Handler) *dockerAPI {
	t.Helper()
	dir, err := os.MkdirTemp("", "kmcdk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return newDockerAPI(sock)
}

const listReply = `[
  {"Names": ["/kmc-node-1"], "State": "running", "NetworkSettings": {"Networks": {"kmc_kmc": {}}}},
  {"Names": ["/kmc-node-2"], "State": "exited",  "NetworkSettings": {"Networks": {"kmc_kmc": {}}}},
  {"Names": ["/kmc-node-3"], "State": "running", "NetworkSettings": {"Networks": {"elsewhere": {}}}},
  {"Names": ["/kmc-node-4"], "State": "running", "NetworkSettings": {"Networks": {"kmc_kmc": {}}}},
  {"Names": ["/kmc-node-5"], "State": "running", "NetworkSettings": {"Networks": {"kmc_kmc": {}}}},
  {"Names": ["/kmc-node-6"], "State": "running", "NetworkSettings": {"Networks": {"kmc_kmc": {}}}},
  {"Names": ["/kmc-node-7"], "State": "running", "NetworkSettings": {"Networks": {"kmc_kmc": {}}}},
  {"Names": ["/unrelated"],  "State": "running", "NetworkSettings": {"Networks": {"bridge": {}}}}
]`

func listHandler(calls *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/containers/json" || r.URL.Query().Get("all") != "1" {
			http.Error(w, `{"message":"unexpected `+r.Method+" "+r.URL.String()+`"}`, http.StatusTeapot)
			return
		}
		calls.Add(1)
		_, _ = w.Write([]byte(listReply))
	})
}

func sevenNodes() map[uint64]Node {
	m := make(map[uint64]Node, 7)
	for i := uint64(1); i <= 7; i++ {
		m[i] = Node{ID: i, ContainerName: fmt.Sprintf("kmc-node-%d", i)}
	}
	return m
}

func TestDockerContainersParsesOneListCall(t *testing.T) {
	var calls atomic.Int32
	d := fakeDocker(t, listHandler(&calls))
	got, err := d.containers(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("want one list call, got %d", calls.Load())
	}
	n1, ok := got["kmc-node-1"]
	if !ok || !n1.Running || n1.Status != "running" || len(n1.Networks) != 1 || n1.Networks[0] != "kmc_kmc" {
		t.Fatalf("node 1 parsed wrong: %+v (present %v)", n1, ok)
	}
	if n2 := got["kmc-node-2"]; n2.Running || n2.Status != "exited" {
		t.Fatalf("node 2 should be exited: %+v", n2)
	}

	e := &Engine{network: "kmc_kmc"}
	node := func(id uint64) Node { return Node{ID: id, ContainerName: fmt.Sprintf("kmc-node-%d", id)} }
	if st := e.statusFrom(node(1), got["kmc-node-1"]); st.Partitioned {
		t.Fatal("node 1 is on the cluster network")
	}
	if st := e.statusFrom(node(2), got["kmc-node-2"]); st.Partitioned {
		t.Fatal("a stopped node is down, not partitioned")
	}
	if st := e.statusFrom(node(3), got["kmc-node-3"]); !st.Partitioned {
		t.Fatal("node 3 is running off the cluster network, so partitioned")
	}
}

func TestDockerStopStartTreatAlreadyInStateAsSuccess(t *testing.T) {
	d := fakeDocker(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/containers/kmc-node-1/stop" && r.URL.Query().Get("t") == "1":
			w.WriteHeader(http.StatusNotModified) // already stopped
		case r.Method == http.MethodPost && r.URL.Path == "/containers/kmc-node-1/start":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, `{"message":"unexpected"}`, http.StatusTeapot)
		}
	}))
	if err := d.stop(t.Context(), "kmc-node-1", 1); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := d.start(t.Context(), "kmc-node-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func TestDockerErrorCarriesDaemonMessage(t *testing.T) {
	d := fakeDocker(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"No such container: kmc-node-9"}`))
	}))
	err := d.start(t.Context(), "kmc-node-9")
	if err == nil || !strings.Contains(err.Error(), "404") || !strings.Contains(err.Error(), "No such container: kmc-node-9") {
		t.Fatalf("want 404 with the daemon's message, got %v", err)
	}
}

func TestConnectNetworkToleratesAlreadyAttached(t *testing.T) {
	d := fakeDocker(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Container string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Method != http.MethodPost || r.URL.Path != "/networks/kmc_kmc/connect" || body.Container != "kmc-node-1" {
			http.Error(w, `{"message":"unexpected"}`, http.StatusTeapot)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"endpoint with name kmc-node-1 already exists in network kmc_kmc"}`))
	}))
	e := &Engine{network: "kmc_kmc", docker: d}
	if err := e.connectNetwork(t.Context(), "kmc-node-1"); err != nil {
		t.Fatalf("already attached should be success, got %v", err)
	}
}

func TestListProbesNodesInParallelAndKeepsOrder(t *testing.T) {
	const probeDelay = 150 * time.Millisecond
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(probeDelay)
		id, _ := strconv.Atoi(r.URL.Query().Get("id"))
		_ = json.NewEncoder(w).Encode(map[string]any{"term": id * 10, "role": "follower"})
	}))
	defer probe.Close()

	var calls atomic.Int32
	e := &Engine{
		docker:  fakeDocker(t, listHandler(&calls)),
		network: "kmc_kmc",
		nodes:   sevenNodes(),
		heals:   make(map[uint64]*healJob),
		probe:   newProbeClient(),
		probeURL: func(id uint64) string {
			return fmt.Sprintf("%s/healthz?id=%d", probe.URL, id)
		},
	}
	start := time.Now()
	out, err := e.List(t.Context())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	// Five probed nodes one after another would take 750ms.
	if elapsed > 600*time.Millisecond {
		t.Fatalf("probes look serial: List took %v", elapsed)
	}
	if calls.Load() != 1 {
		t.Fatalf("want one docker list call per List, got %d", calls.Load())
	}
	for i, st := range out {
		id := uint64(i + 1)
		if st.ID != id {
			t.Fatalf("order broken at %d: %+v", i, st)
		}
		probed := id != 2 && id != 3 // 2 is stopped, 3 is partitioned
		if probed && st.Term != id*10 {
			t.Fatalf("node %d: want term %d from its probe, got %+v", id, id*10, st)
		}
		if !probed && st.Term != 0 {
			t.Fatalf("node %d should not be probed: %+v", id, st)
		}
	}
}
