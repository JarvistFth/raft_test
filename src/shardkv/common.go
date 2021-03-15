package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout	   = "Timeout"
)

const (
	OP_PUT    = "Put"
	OP_GET    = "Get"
	OP_APPEND = "Append"
	OP_GC     = "GC"
)

type Msg string


// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ClientID int64
	RequestID int64
}

type PutAppendReply struct {
	Msg Msg
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientID int64
	RequestID int64
}

type GetReply struct {
	Msg   Msg
	Value string
}

type MigrateArgs struct {
	ServerName		string
	ShardNums	Set
	ConfigNum	int
}



type MigrateReply struct {
	Msg	Msg
	ShardNums	Set
	ConfigNum	int

	//the data to migrate
	Data	map[int]map[string]string

	//client id -> request id
	ReqMap	map[int64]int64


}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string

	ClientId int64
	RequestId int64
}

func (o Op) Equal(op Op) bool {
	return o.Key == op.Key && o.OpType == op.OpType && o.ClientId == op.ClientId && o.RequestId == op.RequestId
}

func (o Op) String() string{
	return fmt.Sprintf("OpType:%s, Key:%s, Value:%s",o.OpType,o.Key,o.Value )
}