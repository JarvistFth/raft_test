package shardkv

import (
	"lab6.824/shardmaster"
	"sync"
)

func (kv *ShardKV) pullShardsFromOthers() {
	_,isLeader := kv.rf.GetState()
	if !isLeader{
		return
	}
	// no new shard, return

	kv.mu.Lock()
	log.Debugf("server:%s, len:%d", kv.serverName,len(kv.shardsToPull))
	if len(kv.shardsToPull) <= 0{
		kv.mu.Unlock()
		return
	}

	var wait sync.WaitGroup


	for cfgNum, shards := range kv.shardsToPull{
		wait.Add(1)

		go func(shards Set, cfg shardmaster.Config) {
			defer wait.Done()
			kv.mu.Lock()
			args := MigrateArgs{
				ServerName: kv.serverName,
				ShardNums:   shards,
				ConfigNum:  cfg.Num,
			}

			gids := Set{}

			for val,_ := range shards{
				shard := val.(int)
				gid := cfg.Shards[shard]
				gids.Add(gid)
			}
			kv.mu.Unlock()
			for val,_ := range gids{
				gid := val.(int)
				for _,server := range cfg.Groups[gid]{
					srv := kv.makeEnd(server)
					reply := MigrateReply{}
					ok := srv.Call("ShardKV.ShardMigration",&args,&reply)
					log.Debugf("server:%s call MigrationRPC to server [%s], reply:%s",kv.serverName, server,reply.Msg)
					if ok && reply.Msg == OK{
						kv.rf.Start(reply)
					}
				}
			}

		}(shards,kv.masterClient.Query(cfgNum))
	}

	kv.mu.Unlock()
	wait.Wait()
}



func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply* MigrateReply)  {
	reply.Msg = ErrWrongLeader
	reply.ConfigNum = args.ConfigNum

	kv.mu.Lock()
	reply.ShardNums = args.ShardNums
	kv.mu.Unlock()

	_,isLeader := kv.rf.GetState()

	if !isLeader{
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Msg = ErrWrongGroup
	log.Debugf("ShardMigration from:%s to me:%s: args:%d, kv.cfg:%s", args.ServerName, kv.serverName, args.ConfigNum, kv.cfg.String())
	log.Debug("dbWait: ",kv.dbWaitToPull)
	if args.ConfigNum >= kv.cfg.Num{
		//cfg is updated, needn't give my db out
		return
	}

	reply.Msg, reply.ConfigNum, reply.ShardNums = OK, args.ConfigNum,args.ShardNums
	reply.Data = make(map[int]map[string]string)
	for val,_ := range reply.ShardNums{
		shard := val.(int)
		if reply.Data[shard] == nil{
			reply.Data[shard] = make(map[string]string)
		}
		reply.Data[shard] = kv.splitDB(args.ConfigNum,shard)
	}
	reply.ReqMap = kv.deepCopyReqMap()
	log.Debug("replyData: ",reply.Data)

}

// lock before already
func (kv *ShardKV) splitDB(configNum , shard int) map[string]string {
	ret := make(map[string]string)

	for k,v := range kv.dbWaitToPull[configNum][shard]{
		ret[k] = v
	}
	return ret
}

// lock before already
func (kv *ShardKV) deepCopyReqMap() map[int64]int64{
	ret := make(map[int64]int64)

	for k,v := range kv.ReqMap{
		ret[k] = v
	}
	return ret
}


func (kv *ShardKV) updateDBWithMigration(migration MigrateReply)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log.Infof("me:%s, migration.config:%d, kv.cfg:%d",kv.serverName,migration.ConfigNum,kv.cfg.Num)
	log.Info("shardToPull",kv.shardsToPull)

	if migration.ConfigNum > kv.cfg.Num {
		return
	}

	log.Infof("me:%s",kv.serverName)
	log.Info("migrationData:",migration.Data)
	log.Infof("myshard:%s, migrationShards:%s",kv.myshards.String(), migration.ShardNums.String())

	for val,_ := range migration.ShardNums{
		shard := val.(int)

		for k,v := range migration.Data[shard]{
			kv.db[shard][k] = v
		}
		kv.shardsToPull[migration.ConfigNum].Remove(shard)
		kv.myshards.Add(shard)
	}
	delete(kv.shardsToPull,migration.ConfigNum)

	log.Debugf("my:%s shardtopull:%d",kv.serverName,len(kv.shardsToPull))
	log.Debug(kv.shardsToPull)

	for clientid,reqid := range migration.ReqMap{
		kv.ReqMap[clientid] = MaxInt64(reqid,kv.ReqMap[clientid])
	}

	//
	//	//if _, ok := kv.Garbages[migration.ConfigNum];!ok{
	//	//	kv.Garbages[migration.ConfigNum] = make(map[int]bool)
	//	//}
	//
	//	//kv.Garbages[migration.ConfigNum][migration.ShardNum] = true
	//	//todo gc here
	//}
}


