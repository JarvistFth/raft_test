package shardkv

import (
	"bytes"
	"lab6.824/labgob"
	"lab6.824/shardmaster"
)

func (kv *ShardKV) takeSnapshotOnDB(index int) {
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)

	kv.mu.Lock()
	_ = e.Encode(index)
	_ = e.Encode(kv.db)
	_ = e.Encode(kv.ReqMap)
	_ = e.Encode(kv.cfg)
	_ = e.Encode(kv.myshards)
	_ = e.Encode(kv.shardsToPull)
	_ = e.Encode(kv.dbWaitToPull)
	_ = e.Encode(kv.Garbages)
	log.Infof("take snapshot, index:%d",index)
	kv.mu.Unlock()
	kv.rf.TakeSnapshot(index,b.Bytes())
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1{
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)

	var idx int
	var db map[int]map[string]string
	var ReqMap map[int64]int64
	var cfg shardmaster.Config
	var myshards Set
	var shardsWaitToPull map[int]Set
	var dbToMigrate map[int]map[int]map[string]string
	var garbage map[int]map[int]bool

	//_ = e.Encode(kv.db)
	//_ = e.Encode(kv.ReqMap)
	//_ = e.Encode(kv.cfg)
	//_ = e.Encode(kv.myshards)
	//_ = e.Encode(kv.shardsToPull)
	//_ = e.Encode(kv.dbWaitToPull)
	//_ = e.Encode(kv.Garbages)

	if d.Decode(&idx) != nil || d.Decode(&db) != nil || d.Decode(&ReqMap) != nil || d.Decode(&cfg) != nil ||
		d.Decode(&myshards) != nil || d.Decode(&shardsWaitToPull) != nil || d.Decode(&dbToMigrate) != nil || d.Decode(&garbage) != nil{

		log.Error("could not decode db && client-req map!!")
	}else{
		index := idx
		kv.db = db
		kv.ReqMap = ReqMap
		kv.cfg = cfg
		kv.myshards = myshards
		kv.shardsToPull = shardsWaitToPull
		kv.dbWaitToPull = dbToMigrate
		kv.Garbages = garbage
		log.Infof(" restore snapshot:%d, myshards:%s",index,myshards)
		log.Debug(kv.db)
	}
}

func (kv *ShardKV) needSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.maxraftstate > 0 &&  kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate/10
}