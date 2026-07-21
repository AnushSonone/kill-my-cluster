package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raftpb"
)

const (
	// Max entries sharing one leader WAL fsync.
	proposeBatchMax = 64
	// How long to wait for more Proposes after the first arrives.
	proposeBatchWait = 500 * time.Microsecond

	// How long to coalesce concurrent ReadIndex callers.
	readIndexBatchWait = 1 * time.Millisecond
)

// runProposeBatcher group-commits concurrent Proposes: append many entries,
// one fsync, one AppendEntries broadcast.
func (n *Node) runProposeBatcher() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case first, ok := <-n.propCh:
			if !ok {
				return
			}
			batch := n.collectProps(first)
			n.commitPropBatch(batch)
		}
	}
}

func (n *Node) collectProps(first propReq) []propReq {
	batch := []propReq{first}
	timer := time.NewTimer(proposeBatchWait)
	defer timer.Stop()
	for len(batch) < proposeBatchMax {
		select {
		case <-n.done:
			return batch
		case req := <-n.propCh:
			batch = append(batch, req)
		case <-timer.C:
			return batch
		}
	}
	return batch
}

func (n *Node) failProps(batch []propReq) {
	for _, req := range batch {
		req.ch <- propResult{isLeader: false}
	}
}

func (n *Node) commitPropBatch(batch []propReq) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		n.failProps(batch)
		return
	}

	term := n.currentTerm
	startIndex := n.log.lastIndex() + 1
	entries := make([]*raftpb.LogEntry, 0, len(batch))
	results := make([]propResult, len(batch))
	for i, req := range batch {
		index := n.log.lastIndex() + 1
		entry := &raftpb.LogEntry{Index: index, Term: term, Data: req.data}
		n.log.append(entry)
		entries = append(entries, entry)
		results[i] = propResult{index: index, term: term, isLeader: true}
	}

	// One fsync for the whole batch (group commit). Hold mu across disk so
	// another Propose batch cannot interleave indexes with an unsynced prefix.
	err := n.persist.appendEntries(entries)
	if err != nil {
		n.log.truncateFrom(startIndex)
		n.mu.Unlock()
		n.failProps(batch)
		return
	}
	n.mu.Unlock()

	n.broadcastAppendEntries()
	for i, req := range batch {
		req.ch <- results[i]
	}
}

// runReadIndexBatcher coalesces ReadIndex callers behind one majority
// heartbeat + apply barrier.
func (n *Node) runReadIndexBatcher() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case first, ok := <-n.readCh:
			if !ok {
				return
			}
			batch := n.collectReads(first)
			n.serveReadBatch(batch)
		}
	}
}

func (n *Node) collectReads(first readReq) []readReq {
	batch := []readReq{first}
	timer := time.NewTimer(readIndexBatchWait)
	defer timer.Stop()
	for {
		select {
		case <-n.done:
			return batch
		case req := <-n.readCh:
			batch = append(batch, req)
		case <-timer.C:
			return batch
		}
	}
}

func (n *Node) failReads(batch []readReq) {
	for _, req := range batch {
		req.ch <- readResult{ok: false}
	}
}

func (n *Node) serveReadBatch(batch []readReq) {
	ctx := context.Background()
	for _, req := range batch {
		if req.ctx != nil {
			ctx = req.ctx
			break
		}
	}

	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		n.failReads(batch)
		return
	}
	term := n.currentTerm
	readIndex := n.commitIndex
	leaseOK := time.Now().Before(n.readLeaseUntil)
	n.mu.Unlock()

	if !leaseOK {
		if !n.confirmLeadership(ctx, term) {
			n.failReads(batch)
			return
		}
		n.mu.Lock()
		// Lease covers a fraction of the election timeout so a partitioned
		// leader cannot serve stale linearizable reads for long.
		n.readLeaseUntil = time.Now().Add(electionTimeoutMin / 3)
		stillLeader := n.role == Leader && n.currentTerm == term
		if stillLeader && n.commitIndex > readIndex {
			readIndex = n.commitIndex
		}
		n.mu.Unlock()
		if !stillLeader {
			n.failReads(batch)
			return
		}
	}

	if !n.WaitApplied(ctx, readIndex) {
		n.failReads(batch)
		return
	}

	res := readResult{index: readIndex, ok: true}
	for _, req := range batch {
		req.ch <- res
	}
}

// confirmLeadership sends a heartbeat round and waits for a majority of the
// cluster (including self) to acknowledge the leader's current term.
func (n *Node) confirmLeadership(ctx context.Context, term uint64) bool {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return false
	}
	peers := append([]uint64(nil), n.peers...)
	clusterSize := len(peers) + 1
	majority := clusterSize/2 + 1
	n.mu.Unlock()

	if len(peers) == 0 {
		return true
	}

	var acks atomic.Int32
	acks.Store(1) // self

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer uint64) {
			defer wg.Done()
			if n.heartbeatAck(ctx, peer, term) {
				acks.Add(1)
			}
		}(peer)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(500 * time.Microsecond)
	defer ticker.Stop()
	for {
		if int(acks.Load()) >= majority {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-n.done:
			return false
		case <-done:
			return int(acks.Load()) >= majority
		case <-ticker.C:
		}
	}
}

// heartbeatAck sends an empty AppendEntries and returns whether the peer
// accepted this leader for term.
func (n *Node) heartbeatAck(ctx context.Context, peer uint64, term uint64) bool {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return false
	}
	next := n.nextIndex[peer]
	if next <= n.log.firstIndex() {
		// Lagging past compaction — skip for ReadIndex confirm.
		n.mu.Unlock()
		return false
	}
	prevIdx := next - 1
	prevTerm, _ := n.log.term(prevIdx)
	req := &raftpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      nil,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.transport.AppendEntries(ctx, peer, req)
	if err != nil || resp == nil {
		return false
	}
	if resp.Term > term {
		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term)
		}
		n.mu.Unlock()
		return false
	}
	return resp.Success && resp.Term == term
}
