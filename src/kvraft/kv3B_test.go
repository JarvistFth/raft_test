package kvraft

import (
	"strconv"
	"testing"
	"time"
)

const maxraftstate = 500

func TestSnapshotRPC3B(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: InstallSnapshot RPC (3B)")

	Put(cfg, ck, "a", "A")
	Log().Debug.Printf("check")
	check(cfg, t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			Log().Debug.Printf("i:%d",i)
			Put(cfg, ck1, strconv.Itoa(i), strconv.Itoa(i))
		}
		time.Sleep(electionTimeout)
		Put(cfg, ck1, "b", "B")
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		Put(cfg, ck1, "c", "C")
		Put(cfg, ck1, "d", "D")
		check(cfg, t, ck1, "a", "A")
		check(cfg, t, ck1, "b", "B")
		check(cfg, t, ck1, "1", "1")
		check(cfg, t, ck1, "49", "49")
	}

	// now everybody
	Log().Warning.Printf("now every body...")
	cfg.partition([]int{0, 1, 2}, []int{})
	Put(cfg, ck, "e", "E")
	check(cfg, t, ck, "c", "C")
	check(cfg, t, ck, "e", "E")
	check(cfg, t, ck, "1", "1")

	cfg.end()
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
func TestSnapshotSize3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	maxsnapshotstate := 500
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: snapshot size is reasonable (3B)")

	for i := 0; i < 200; i++ {
		Put(cfg, ck, "x", "0")
		check(cfg, t, ck, "x", "0")
		Put(cfg, ck, "x", "1")
		check(cfg, t, ck, "x", "1")
	}

	// check that servers have thrown away most of their log entries
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// check that the snapshots are not unreasonably large
	ssz := cfg.SnapshotSize()
	if ssz > maxsnapshotstate {
		t.Fatalf("snapshot too large (%v > %v)", ssz, maxsnapshotstate)
	}

	cfg.end()
}

func TestSnapshotRecover3B(t *testing.T) {
	// Test: restarts, snapshots, one client (3B) ...
	GenericTest(t, "3B", 1, false, true, false, 1000)
}

func TestSnapshotRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 20, false, true, false, 1000)
}

func TestSnapshotUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, false, false, 1000)
}

func TestSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, false, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, true, true, true, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, linearizability checks (3B) ...
	GenericTestLinearizability(t, "3B", 15, 7, true, true, true, 1000)
}
