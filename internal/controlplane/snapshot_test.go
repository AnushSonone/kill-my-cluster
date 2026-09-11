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

// ageSnapshot makes the cached view look older than it is.
func ageSnapshot(e *Engine, d time.Duration) {
	e.snapMu.Lock()
	e.snapAt = time.Now().Add(-d)
	e.snapMu.Unlock()
}

func TestSnapshotServesSlightlyStaleWithoutWaiting(t *testing.T) {
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
	ageSnapshot(e, 700*time.Millisecond)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := e.Snapshot(context.Background()); got.Total != 1 {
				t.Errorf("a view under the stale limit is served while rebuilding: got %+v", got)
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

func TestSnapshotWaitsForABuildAfterAnAction(t *testing.T) {
	var builds atomic.Int32
	e := &Engine{buildFn: func(ctx context.Context) Snapshot {
		time.Sleep(30 * time.Millisecond)
		return Snapshot{Total: int(builds.Add(1))}
	}}
	_ = e.Snapshot(t.Context())
	e.invalidateSnapshot() // a kill just happened
	if got := e.Snapshot(t.Context()); got.Total != 2 {
		t.Fatalf("the next caller after an action must see a new build, got %+v", got)
	}
}

func TestSnapshotWaitsWhenPastTheStaleLimit(t *testing.T) {
	var builds atomic.Int32
	e := &Engine{buildFn: func(ctx context.Context) Snapshot {
		return Snapshot{Total: int(builds.Add(1))}
	}}
	_ = e.Snapshot(t.Context())
	ageSnapshot(e, 3*time.Second) // nobody asked for a while
	if got := e.Snapshot(t.Context()); got.Total != 2 {
		t.Fatalf("a view past the stale limit must not be served, got %+v", got)
	}
}

func TestWaitingCallersShareOneBuild(t *testing.T) {
	var builds atomic.Int32
	e := &Engine{buildFn: func(ctx context.Context) Snapshot {
		n := builds.Add(1)
		time.Sleep(50 * time.Millisecond)
		return Snapshot{Total: int(n)}
	}}
	_ = e.Snapshot(t.Context())
	e.invalidateSnapshot()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := e.Snapshot(context.Background()); got.Total != 2 {
				t.Errorf("every waiter gets the shared build: got %+v", got)
			}
		}()
	}
	wg.Wait()
	if n := builds.Load(); n != 2 {
		t.Fatalf("16 waiters must share one build, got %d builds", n)
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

func TestInvalidateDuringBuildMakesTheNextCallerWait(t *testing.T) {
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
	if got := e.Snapshot(t.Context()); got.Total != 3 {
		t.Fatalf("build 2 predates the kill, so the next caller waits for build 3; got %+v", got)
	}
}

func TestSharedBuildIgnoresCallerCancellation(t *testing.T) {
	var cancelled atomic.Bool
	e := &Engine{buildFn: func(ctx context.Context) Snapshot {
		if ctx.Err() != nil {
			cancelled.Store(true)
		}
		return Snapshot{Total: 1}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = e.SnapshotFresh(ctx)
	if cancelled.Load() {
		t.Fatal("a build shared by every waiter must not run on one caller's cancelled context")
	}
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
