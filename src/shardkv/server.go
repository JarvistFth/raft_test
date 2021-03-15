package shardkv

import (
	"lab6.824/labrpc"
	"lab6.824/raft"
	"lab6.824/shardmaster"
	"sync"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big



	mygid        int
	masters      []*labrpc.ClientEnd
	masterClient *shardmaster.Clerk
	cfg          shardmaster.Config
	serverName	 string
	// Your definitions here.

	persister *raft.Persister
	killCh	chan struct{}

	db map[int]map[string]string

	//raft op index -> op
	OpMap map[int]chan Op

	//clientId -> max requestId
	ReqMap map[int64]int64

	dbWaitToPull         map[int]map[int]map[string]string //num->shard->db
	//shardsPullFromOthers map[int]int                       //shard -> config num
	shardsToPull		map[int]Set                     //cfg num -> shards[]

	oldshards Set
	newshards []int
	myshards   Set //shards belongs with me

	Garbages 	map[int]map[int]bool   //cfg num -> shard -> is garbages?
}



//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	kv.killCh <- struct{}{}
}
