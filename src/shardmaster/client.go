package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	lastServer int

	ClientId int64
	RequestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.ClientId = nrand()
	ck.RequestId = 0
	ck.lastServer = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num}
	// Your code here.
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply QueryReply
	//		ok := srv.Call("ShardMaster.Query", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			ck.lastServer =
	//			return reply.Config
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
	i := ck.lastServer
	var reply QueryReply
	for{
		//Log().Info.Printf("client %d , send get rpc to server %d",ck.ClientId,i)
		ok := ck.servers[i].Call("ShardMaster.Query",&args,&reply)
		if ok && reply.WrongLeader == false{
			ck.lastServer = i
			return reply.Config

		}
		i = (i+1) % len(ck.servers)
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers,ClientId: ck.ClientId,RequestId: ck.RequestId}
	ck.RequestId++
	// Your code here.
	args.Servers = servers

	i := ck.lastServer
	var reply JoinReply
	for{
		//Log().Info.Printf("client %d , send get rpc to server %d",ck.ClientId,i)
		ok := ck.servers[i].Call("ShardMaster.Join",&args,&reply)
		if ok && reply.WrongLeader == false{
			ck.lastServer = i
			return

		}
		i = (i+1) % len(ck.servers)
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}
	ck.RequestId++
	// Your code here.
	var reply LeaveReply
	i := ck.lastServer
	for{
		//Log().Info.Printf("client %d , send get rpc to server %d",ck.ClientId,i)
		ok := ck.servers[i].Call("ShardMaster.Leave",&args,&reply)
		if ok && reply.WrongLeader == false{
			ck.lastServer = i
			return

		}
		i = (i+1) % len(ck.servers)
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}
	ck.RequestId++
	// Your code here.

	var reply MoveReply
	i := ck.lastServer
	for{
		//Log().Info.Printf("client %d , send get rpc to server %d",ck.ClientId,i)
		ok := ck.servers[i].Call("ShardMaster.Move",&args,&reply)
		if ok && reply.WrongLeader == false{
			ck.lastServer = i
			return

		}
		i = (i+1) % len(ck.servers)
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}
