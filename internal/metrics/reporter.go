package metrics

import (
	"context"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

// RaftSource is anything that can report Raft gauges (usually *raft.Node).
type RaftSource interface {
	Status() (term uint64, isLeader bool)
	Role() raft.Role
	CommitIndex() uint64
	LeaderID() uint64
}

// Reporter periodically copies Raft state into Prometheus gauges.
type Reporter struct {
	src  RaftSource
	col  *Collector
	every time.Duration
}

// NewReporter polls src every interval (default 500ms).
func NewReporter(src RaftSource, col *Collector, every time.Duration) *Reporter {
	if every <= 0 {
		every = 500 * time.Millisecond
	}
	return &Reporter{src: src, col: col, every: every}
}

// Run blocks until ctx is cancelled.
func (r *Reporter) Run(ctx context.Context) {
	t := time.NewTicker(r.every)
	defer t.Stop()
	r.sample()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sample()
		}
	}
}

func (r *Reporter) sample() {
	term, isLeader := r.src.Status()
	r.col.SetRaft(term, isLeader, RoleInt(r.src.Role().String()), r.src.CommitIndex(), r.src.LeaderID())
}
