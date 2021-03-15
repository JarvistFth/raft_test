package kvraft

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
	// You'll have to add code here.

	ck.ClientId = nrand()
	ck.RequestId  = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key,ClientID: ck.ClientId,RequestID: ck.RequestId}
	reply := GetReply{}
	i:= ck.lastServer
	ck.RequestId++
	for{
		//Log().Info.Printf("client %d , send get rpc to server %d",ck.ClientId,i)
		ok := ck.servers[i].Call("KVServer.Get",&args,&reply)
		if ok && reply.Msg == OK{
				ck.lastServer = i
				return reply.Value

		}
		i = (i+1) % len(ck.servers)
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientID: ck.ClientId,
		RequestID: ck.RequestId,
	}

	ck.RequestId++

	reply := PutAppendReply{}
	i := ck.lastServer
	for{
		//Log().Info.Printf("client %d , send put rpc to server %d",ck.ClientId,i)
		ok := ck.servers[i].Call("KVServer.PutAppend",&args,&reply)
		if ok && reply.Msg == OK{
			ck.lastServer = i
			return
		}
		i = (i+1) % len(ck.servers)
		time.Sleep(time.Duration(100)*time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
