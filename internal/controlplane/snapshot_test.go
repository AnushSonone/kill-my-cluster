package controlplane

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSnapshotServesStaleWithoutWaitingOnABuild(t *testing.T) {
	var builds atomic.Int32
	release := make(chan struct{})
	e := &Engine{}
	e.buildFn = func(ctx context.Context) Snapshot {
		n := builds.Add(1)
		if n > 1 {
			<-release
		}
		return Snapshot{Total: int(n)}
	}

	if got := e.Snapshot(t.Context()); got.Total != 1 {
		t.Fatalf("first call builds synchronously: got %+v", got)
	}
	e.invalidateSnapshot()

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := e.Snapshot(context.Background()); got.Total != 1 {
				t.Errorf("while rebuilding, serve the last view: got %+v", got)
			}
		}()
	}
	wg.Wait()
	if waited := time.Since(start); waited > 250*time.Millisecond {
		t.Fatalf("callers waited on the build: %v", waited)
	}
	eventually(t, "one background rebuild to start", func() bool { return builds.Load() == 2 })

	close(release)
	eventually(t, "the rebuilt view to be served", func() bool { return e.Snapshot(t.Context()).Total == 2 })
	if n := builds.Load(); n != 2 {
		t.Fatalf("32 concurrent callers must start exactly one rebuild, got %d builds", n)
	}
}

func TestSnapshotFreshBuildsWhenStaleAndReusesWhenFresh(t *testing.T) {
	var builds atomic.Int32
	e := &Engine{buildFn: func(ctx context.Context) Snapshot {
		return Snapshot{Total: int(builds.Add(1))}
	}}
	_ = e.Snapshot(t.Context())
	e.invalidateSnapshot()
	if got := e.SnapshotFresh(t.Context()); got.Total != 2 {
		t.Fatalf("fresh after invalidate must rebuild: got %+v", got)
	}
	if got := e.SnapshotFresh(t.Context()); got.Total != 2 || builds.Load() != 2 {
		t.Fatalf("a view under 500ms old is fresh enough: got %+v after %d builds", got, builds.Load())
	}
}

func TestInvalidateDuringBuildLeavesViewStale(t *testing.T) {
	var builds atomic.Int32
	e := &Engine{}
	e.buildFn = func(ctx context.Context) Snapshot {
		n := builds.Add(1)
		if n == 2 {
			e.invalidateSnapshot() // a kill lands while this build is reading docker
		}
		return Snapshot{Total: int(n)}
	}
	_ = e.Snapshot(t.Context())
	e.invalidateSnapshot()
	if got := e.SnapshotFresh(t.Context()); got.Total != 2 {
		t.Fatalf("want build 2, got %+v", got)
	}
	_ = e.Snapshot(t.Context()) // build 2 predates the kill, so this must trigger build 3
	eventually(t, "a rebuild after the mid-build change", func() bool { return builds.Load() == 3 })
}

func TestSnapshotJSONMatchesSnapshot(t *testing.T) {
	e := &Engine{buildFn: func(ctx context.Context) Snapshot {
		return Snapshot{Total: 7, Alive: 6, Quorum: true, Nodes: []Status{{ID: 1, Running: true, Role: "leader"}}}
	}}
	var got Snapshot
	if err := json.Unmarshal(e.SnapshotJSON(t.Context()), &got); err != nil {
		t.Fatal(err)
	}
	if got.Total != 7 || got.Alive != 6 || !got.Quorum || len(got.Nodes) != 1 || got.Nodes[0].Role != "leader" {
		t.Fatalf("encoded view differs: %+v", got)
	}
}
