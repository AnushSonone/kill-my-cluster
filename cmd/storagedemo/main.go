// Command storagedemo is a tiny, hands-on demonstration of crash recovery in the
// Phase 1 storage engine.
//
// What it does: it opens a persistent key-value store in a fixed temp directory,
// reads an integer "counter" key (defaulting to 0 the very first time, when the
// key does not yet exist), prints the value it recovered, then increments and
// durably writes the counter five times, printing each new value.
//
// Why it's interesting: run it once and you'll see the counter climb from 0 to
// 5. Run it AGAIN — or kill it with Ctrl-C partway through and restart it — and
// the counter resumes exactly where it left off instead of starting over. That
// continuity is the whole point of a write-ahead log: every Put is appended to
// wal.log and fsync'd to stable storage BEFORE Put returns, so an acknowledged
// write survives the process exiting for any reason. On the next Open, the store
// replays that log (on top of any snapshot) and reconstructs the state you had
// promised. This is the single-node foundation on which real distributed systems
// build replication and consensus.
//
// Try it:
//
//	go run ./cmd/storagedemo   # prints "recovered counter = 0", then 1..5
//	go run ./cmd/storagedemo   # prints "recovered counter = 5", then 6..10
package main

import (
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/AnushSonone/kill-my-cluster/internal/storage"
)

func main() {
	// A stable location so state persists across separate runs of this program.
	// (A real deployment would use a configured data directory, not TempDir.)
	dir := filepath.Join(os.TempDir(), "kmc-storagedemo")

	store, err := storage.Open(dir)
	if err != nil {
		log.Fatalf("open store at %q: %v", dir, err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			log.Fatalf("close store: %v", cerr)
		}
	}()

	// Recover the counter. A missing key (first ever run) means "start at 0".
	counter := 0
	if raw, ok := store.Get("counter"); ok {
		n, perr := strconv.Atoi(string(raw))
		if perr != nil {
			log.Fatalf("stored counter %q is not an integer: %v", raw, perr)
		}
		counter = n
	}
	log.Printf("recovered counter = %d", counter)

	// Increment and durably persist five times. Because each Put is fsync'd
	// before it returns, every printed value below is already safe on disk — even
	// if the machine loses power on the next line.
	for i := 0; i < 5; i++ {
		counter++
		if err := store.Put("counter", []byte(strconv.Itoa(counter))); err != nil {
			log.Fatalf("put counter=%d: %v", counter, err)
		}
		log.Printf("counter = %d (durably written)", counter)
	}
}
