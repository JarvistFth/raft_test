package shardkv

import (
	"lab6.824/raft"
	"lab6.824/shardmaster"
)

func handleRequest(op Op){
	//shard := key2shard(op.Key)

}

func (kv *ShardKV) handleApplyMsg(msg raft.ApplyMsg) {
	cmd := msg.Command
	switch cmd.(type) {
	case shardmaster.Config:
		newcfg := cmd.(shardmaster.Config)
		log.Warningf("gid:%d - server[%d]: get new cfg: %s",kv.mygid, kv.me ,newcfg.String())
		kv.updateShards(newcfg)

		//1. update config
		//2. try pull shard data

		break

	case MigrateReply:
		log.Debugf("me:%s, applymsg index:%d", kv.serverName,msg.CommandIndex)
		kv.updateDBWithMigration(cmd.(MigrateReply))
		//migrate msg
		//update db, migrate db to others
		break

	case Op:
		op := cmd.(Op)
		//Log().Debug.Printf("get msg from apply ch, idx: %d, value:%s", msg.CommandIndex,op.Value)

		shard := key2shard(op.Key)
		clientid := op.ClientId

		kv.mu.Lock()
		if !kv.myshards.Contain(shard){
			log.Warningf("op:%s key shard:%d, but me:%s myshards:%s  ",op.String(), shard,kv.serverName ,kv.myshards.String())
			op.OpType = ErrWrongGroup
		}else{
			reqNum,found := kv.ReqMap[clientid]

			//kv.db as state machine, dont use same cmd on state machine
			switch op.OpType {
			case OP_PUT:
				if !found || op.RequestId > reqNum {
					//log.Warningf("me:%s, try put shard:%d, key:%v, value:%v", kv.serverName,shard,op.Key, op.Value)
					kv.db[shard][op.Key] = op.Value
					kv.ReqMap[op.ClientId] = op.RequestId
				}
			case OP_APPEND:
				if !found || op.RequestId > reqNum {
					//log.Warningf("me:%s, try append shard:%d, key:%v, value:%v", kv.serverName,shard,op.Key, op.Value)
					kv.db[shard][op.Key] += op.Value
					kv.ReqMap[op.ClientId] = op.RequestId
				}
			case OP_GET:
				log.Infof("myshards: %s",kv.myshards)
				//log.Info("db:",kv.db)
				op.Value,_ = kv.db[shard][op.Key]
				log.Warningf("me:%s, try get shard:%d, key:%v, value:%v", kv.serverName,shard,op.Key, op.Value)
			case OP_GC:
				//kv.LocalGC(op.Key,op.Value)
				//log.Debug("local gc..")
			}
		}
		kv.mu.Unlock()
		index := msg.CommandIndex
		ch := kv.getOpIndexCh(index,false)
		if ch != nil{
			sendCh(ch,op)
		}

	}
	if kv.needSnapshot(){
		//Log().Debug.Printf("before log size:%d",kv.persister.RaftStateSize())
		//like redis db file saveState, new goroutine to take snapshot
		go kv.takeSnapshotOnDB(msg.CommandIndex)
		//kv.takeSnapshotOnDB(index)
		//Log().Debug.Printf("after log size:%d",kv.persister.RaftStateSize())
	}

}
