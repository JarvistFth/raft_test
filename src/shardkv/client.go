package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"lab6.824/labrpc"
	"lab6.824/logger"
	"lab6.824/shardmaster"
)
import "math/big"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//

var log = logger.GetLogger("shardkv")

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm      *shardmaster.Clerk
	config  shardmaster.Config
	makeEnd func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	lastServer int

	ClientId int64
	RequestId int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[mygid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.makeEnd = makeEnd
	// You'll have to add code here.

	ck.ClientId = nrand()
	ck.RequestId  = 0
	ck.lastServer = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientID:  ck.ClientId,
		RequestID: ck.RequestId,
	}

	ck.RequestId++


	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for i := 0; i < len(servers); i++ {
				si := (i+ck.lastServer) % len(servers)
				srv := ck.makeEnd(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Msg == OK || reply.Msg == ErrNoKey) {
					ck.lastServer = si
					return reply.Value
				}
				if ok && (reply.Msg == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientID:  ck.ClientId,
		RequestID: ck.RequestId,
	}

	ck.RequestId++
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for i := 0; i < len(servers); i++ {
				si := (i+ck.lastServer) % len(servers)
				//log.Debugf("si:%d, serverslen:%d",si,len(servers))
				srv := ck.makeEnd(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Msg == OK{
					ck.lastServer = si
					return
				}
				if ok && reply.Msg == ErrWrongGroup {
					log.Error("wrong group, redirect...")
					break
				}
				log.Debugf("try put rpc to server[%s] with shard:%d, rpcok:%t, reply:%s",servers[si], shard,ok,reply.Msg)

				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
