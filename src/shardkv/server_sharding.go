package shardkv

import (
	"lab6.824/shardmaster"
)

//do after raft msg return
func (kv *ShardKV) updateShards(newcfg shardmaster.Config)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if newcfg.Num <= kv.cfg.Num{
		//only the newest new cfg should process
		return
	}
	log.Debugf("gid:%d, newcfg: %s cfg: %s", kv.mygid, newcfg.String(),kv.cfg.String())

	oldcfg := kv.cfg
	kv.cfg = newcfg

	kv.newshards = newcfg.GidToShards[kv.mygid]
	oldshards := oldcfg.GidToShards[kv.mygid]

	for _,oldshard := range oldshards{
		kv.oldshards.Add(oldshard)
	}

	log.Debugf("mygid:%d newshards: %d",kv.mygid,kv.newshards)
	log.Debugf("oldshards: %s\n\toldcfg:%s", kv.oldshards.String(), oldcfg.String())

	for _,newshard := range kv.newshards{
		kv.myshards.Add(newshard)
		if !kv.oldshards.Contain(newshard) && oldcfg.Num != 0{
			// need pull from others
			if kv.shardsToPull[oldcfg.Num] == nil{
				kv.shardsToPull[oldcfg.Num] = Set{}
			}
			kv.shardsToPull[oldcfg.Num].Add(newshard)
		}else{
			kv.oldshards.Remove(newshard)
		}
	}

	if !kv.oldshards.Empty(){
		kv.dbWaitToPull[oldcfg.Num] = make(map[int]map[string]string)
	}

	for val,_ := range kv.oldshards{
		shard := val.(int)
		//for shard,shardingdb := range kv.db{
		//	if key2shard(k) == shard{
		//		outdb[k] = v
		//		delete(kv.db,k)
		//	}
		//}
		if kv.dbWaitToPull[oldcfg.Num][shard] == nil{
			kv.dbWaitToPull[oldcfg.Num][shard] = make(map[string]string)
		}
		kv.dbWaitToPull[oldcfg.Num][shard] =  kv.db[shard]

	}

	log.Debugf("after server:%s len(shardsToPull):%d",kv.serverName,len(kv.shardsToPull))
	log.Debug("shard to pull:",kv.shardsToPull)
	// 1. check new shards, remove all new shard belongs to me from shardToOut,
	// then shardToOut will left which shard waiting for pull

	// 2. add those shard belongs to me to kv.myshard

	// 3. otherwise, we have new shard, try pull from others.
	// we will do it in another goroutines

}




func (kv *ShardKV) FetchLatestCfg() {

	_,isLeader := kv.rf.GetState()
	kv.mu.Lock()

	if !isLeader || len(kv.shardsToPull) > 0{
		kv.mu.Unlock()
		return
	}
	nextCfgNum := kv.cfg.Num + 1
	kv.mu.Unlock()
	nextCfg := kv.masterClient.Query(nextCfgNum)
	log.Debugf("gid:%d try FetchLatestCfg[%d]: %s",kv.mygid, nextCfgNum, nextCfg.String())
	if nextCfg.Num == nextCfgNum {
		log.Debugf("gid:%d, start sync cfg: %s", kv.mygid,nextCfg.String())
		kv.rf.Start(nextCfg)
	}
}





