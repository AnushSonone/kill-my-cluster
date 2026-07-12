// raftdemo boots a 3-node Raft cluster on localhost, elects a leader,
// replicates a few writes, kills the leader, and shows sub-second re-election.
// This is the Phase 2 "done" smoke test you can run by hand.
//
// Usage:
//
//	go run ./cmd/raftdemo
package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

const nNodes = 3

type member struct {
	node *raft.Node
	srv  *raft.Server
	ch   chan raft.ApplyMsg
}

func main() {
	base, err := os.MkdirTemp("", "kill-my-cluster-raftdemo-*")
	if err != nil {
		fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(base)
	fmt.Printf("data dir (ephemeral): %s\n\n", base)

	// Reserve stable addresses, same pattern as cluster_test.go.
	addrs := make(map[uint64]string, nNodes)
	listeners := make([]net.Listener, nNodes)
	for i := 0; i < nNodes; i++ {
		id := uint64(i + 1)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fatalf("listen node %d: %v", id, err)
		}
		listeners[i] = lis
		addrs[id] = lis.Addr().String()
	}

	members := make([]member, nNodes)

	for i := 0; i < nNodes; i++ {
		id := uint64(i + 1)
		dir := filepath.Join(base, fmt.Sprintf("node%d", id))

		var peers []uint64
		peerAddrs := make(map[uint64]string)
		for pid, addr := range addrs {
			if pid == id {
				continue
			}
			peers = append(peers, pid)
			peerAddrs[pid] = addr
		}

		ch := make(chan raft.ApplyMsg, 64)
		go drain(ch, id)

		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers, Dir: dir,
			Transport: raft.NewGRPCTransport(peerAddrs),
			ApplyCh:   ch,
		})
		if err != nil {
			fatalf("node %d: %v", id, err)
		}
		// Release the reserved listener, then bind the real server on the
		// same address (stable for the demo's lifetime).
		_ = listeners[i].Close()
		srv, err := raft.NewServer(node, addrs[id])
		if err != nil {
			fatalf("server %d: %v", id, err)
		}
		members[i] = member{node: node, srv: srv, ch: ch}
		fmt.Printf("node %d listening on %s\n", id, srv.Addr())
	}

	fmt.Println("\n--- waiting for leader election ---")
	leader := waitForLeader(members, 3*time.Second)
	fmt.Printf("leader elected: node %d\n", leader)

	fmt.Println("\n--- replicating writes ---")
	propose(members, []byte("write-1"))
	propose(members, []byte("write-2"))
	propose(members, []byte("write-3"))
	waitForCommit(members, 3, 3*time.Second)
	fmt.Println("3 entries committed on a majority")

	fmt.Printf("\n--- killing leader (node %d) ---\n", leader)
	idx := int(leader - 1)
	members[idx].node.Stop()
	members[idx].srv.Stop()
	members[idx].node = nil
	members[idx].srv = nil

	start := time.Now()
	newLeader := waitForLeader(members, 3*time.Second)
	fmt.Printf("new leader: node %d (elected in %v)\n", newLeader, time.Since(start))

	fmt.Println("\n--- write after failover ---")
	propose(members, []byte("survived-kill"))
	waitForCommit(members, 4, 3*time.Second)
	fmt.Println("entry 4 committed — cluster survived leader kill")

	fmt.Println("\n--- shutting down ---")
	for i := range members {
		if members[i].node != nil {
			members[i].node.Stop()
		}
		if members[i].srv != nil {
			members[i].srv.Stop()
		}
	}
	fmt.Println("done.")
}

func drain(ch chan raft.ApplyMsg, id uint64) {
	for msg := range ch {
		if msg.CommandValid {
			fmt.Printf("  [node %d applied] index=%d term=%d data=%q\n",
				id, msg.CommandIndex, msg.CommandTerm, string(msg.Command))
		}
	}
}

func waitForLeader(members []member, timeout time.Duration) uint64 {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m.node == nil {
				continue
			}
			if _, isLeader := m.node.Status(); isLeader {
				return m.node.ID()
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	fatalf("no leader within %v", timeout)
	return 0
}

func propose(members []member, data []byte) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m.node == nil {
				continue
			}
			if idx, term, ok := m.node.Propose(data); ok {
				fmt.Printf("  proposed %q → index=%d term=%d (via node %d)\n",
					string(data), idx, term, m.node.ID())
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	fatalf("no leader accepted %q", data)
}

func waitForCommit(members []member, minIndex uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n := 0
		for _, m := range members {
			if m.node != nil && m.node.CommitIndex() >= minIndex {
				n++
			}
		}
		if n >= 2 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	fatalf("index %d not committed on majority within %v", minIndex, timeout)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
