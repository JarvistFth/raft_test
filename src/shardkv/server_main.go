package shardkv

import (
	"lab6.824/labgob"
	"lab6.824/labrpc"
	"lab6.824/raft"
	"lab6.824/shardmaster"
	"strconv"
	"time"
)

func (kv *ShardKV) AddBackgroundTask(do func(), intervalMs int) {
	for{
		select {
		case <-kv.killCh:
			return
		default:
			do()
		}
		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// mygid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[mygid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	//log.Noticef("make gid:%d",gid)
	kv.mygid = gid
	kv.masters = masters

	kv.masterClient = shardmaster.MakeClerk(kv.masters)
	kv.cfg = shardmaster.Config{}
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.masterClient = shardmaster.MakeClerk(kv.masters)
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan struct{})

	kv.db = make(map[int]map[string]string)
	kv.OpMap = make(map[int]chan Op)
	kv.ReqMap = make(map[int64]int64)

	kv.myshards = Set{}
	kv.oldshards = Set{}
	kv.dbWaitToPull = make(map[int]map[int]map[string]string)
	kv.shardsToPull = make(map[int]Set)
	for i:=0 ; i< shardmaster.NShards;i++{
		kv.db[i] = make(map[string]string)
	}
	kv.Garbages = make(map[int]map[int]bool)
	kv.serverName = "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(kv.me)

	log.Infof("me:%s restore snapshot",kv.serverName)
	kv.restoreSnapshot(kv.persister.ReadSnapshot())

	go kv.AddBackgroundTask(kv.FetchLatestCfg,50)
	go kv.AddBackgroundTask(kv.pullShardsFromOthers,35)
	//go kv.AddBackgroundTask(kv.tryGC,100)


	go func() {
		for{
			select {
			case <- kv.killCh:
				return
			case applyMsg := <- kv.applyCh:
				if !applyMsg.CommandValid{
					kv.restoreSnapshot(applyMsg.SnapShot)
					continue
				}
				kv.handleApplyMsg(applyMsg)
			}
		}
	}()

	return kv
}

