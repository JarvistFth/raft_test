package shardkv

import (
	"lab6.824/models"
	"lab6.824/porcupine"
	"testing"
)
import "strconv"
import "time"
import "fmt"
import "sync/atomic"
import "sync"
import "math/rand"
import "io/ioutil"

const linearizabilityCheckTimeout = 1 * time.Second

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:%v\n\treceived:%v", key, value, v)
	}
}

//
// test static 2-way sharding, without shard movement.
//
func TestStaticShards(t *testing.T) {
	fmt.Printf("Test: static shards ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)
	log.Info("end join 0")

	cfg.join(1)
	log.Info("end join 1")

	time.Sleep(2 * time.Second)
	n := 10
	ka := make([]string, n)
	va := make([]string, n)

	log.Info("start put")
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		//log.Warningf("puti: %d",i)
		ck.Put(ka[i], va[i])
	}

	log.Info("start check - 1..")
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	log.Info("end check - 1..")

	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	log.Warning("\tshutdown group ..")
	cfg.ShutdownGroup(1)
	cfg.checklogs() // forbid snapshots

	ch := make(chan bool)
	for xi := 0; xi < n; xi++ {
		ck1 := cfg.makeClient() // only one call allowed per client
		go func(i int) {
			defer func() { ch <- true }()
			check(t, ck1, ka[i], va[i])
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	ndone := 0
	done := false
	for done == false {
		select {
		case <-ch:
			ndone += 1
		case <-time.After(time.Second * 3):
			done = true
			break
		}
	}

	if ndone != 5 {
		t.Fatalf("expected 5 completions with one shard dead; got %v\n", ndone)
	}

	// bring the crashed shard/group back to life.
	log.Warning("\tbring the crashed shard/group back to life.")
	cfg.StartGroup(1)
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestJoinLeave(t *testing.T) {
	fmt.Printf("Test: join then leave ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	log.Warning("start check")
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	log.Warning("start join g1")

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	log.Warning("start leave g0")

	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	// allow time for shards to transfer.
	time.Sleep(2 * time.Second)

	cfg.checklogs()

	log.Warning("start shutdown g0")
	cfg.ShutdownGroup(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestSnapshot(t *testing.T) {
	fmt.Printf("Test: snapshots, join, and leave ...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	log.Warning("end put")
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)
	log.Warning("check - 0")

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}
	time.Sleep(1 * time.Second)

	cfg.leave(1)
	cfg.join(0)
	log.Warning("check - 1")

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(1 * time.Second)
	log.Warning("check - 2")

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(1 * time.Second)

	cfg.checklogs()
	log.Warning("end check logs")
	log.Warning("start ShutdownGroups")

	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)

	log.Warning("restart Groups")

	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	log.Warning("check restart kv")
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestMissChange(t *testing.T) {
	fmt.Printf("Test: servers miss configuration changes...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	cfg.ShutdownServer(0, 0)
	cfg.ShutdownServer(1, 0)
	cfg.ShutdownServer(2, 0)

	cfg.join(2)
	cfg.leave(1)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 0)
	cfg.StartServer(1, 0)
	cfg.StartServer(2, 0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(2 * time.Second)

	cfg.ShutdownServer(0, 1)
	cfg.ShutdownServer(1, 1)
	cfg.ShutdownServer(2, 1)

	cfg.join(0)
	cfg.leave(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 1)
	cfg.StartServer(1, 1)
	cfg.StartServer(2, 1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent1(t *testing.T) {
	fmt.Printf("Test: concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)

	cfg.ShutdownGroup(0)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(1)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(2)

	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)

	time.Sleep(1 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

//
// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
//
func TestConcurrent2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(0)
	cfg.join(2)
	cfg.leave(1)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(1)
	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)

	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	time.Sleep(1000 * time.Millisecond)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable1(t *testing.T) {
	fmt.Printf("Test: unreliable 1...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(0)
	cfg.leave(1)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable2(t *testing.T) {
	fmt.Printf("Test: unreliable 2...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable3(t *testing.T) {
	fmt.Printf("Test: unreliable 3...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		start := int64(time.Since(begin))
		ck.Put(ka[i], va[i])
		end := int64(time.Since(begin))
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp models.KvInput
			var out models.KvOutput
			start := int64(time.Since(begin))
			if (rand.Int() % 1000) < 500 {
				ck1.Append(ka[ki], nv)
				inp = models.KvInput{Op: 2, Key: ka[ki], Value: nv}
			} else if (rand.Int() % 1000) < 100 {
				ck1.Put(ka[ki], nv)
				inp = models.KvInput{Op: 1, Key: ka[ki], Value: nv}
			} else {
				v := ck1.Get(ka[ki])
				inp = models.KvInput{Op: 0, Key: ka[ki]}
				out = models.KvOutput{Value: v}
			}
			end := int64(time.Since(begin))
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	fmt.Printf("  ... Passed\n")
}

//
// optional test to see whether servers are deleting
// shards for which they are no longer responsible.
//