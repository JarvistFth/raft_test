package shardmaster

import (
	"raft"
)
import "labrpc"
import "sync"
import "labgob"

import "pkg/"

const (
	OP_MOVE = "Move"
	OP_JOIN = "Join"
	OP_LEAVE = "Leave"
	OP_QUERY = "Query"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	OpMap 		map[int]chan Op
	ReqMap		map[int64]int64
	killCh chan struct{}
}

type Op struct {
	// Your data here.

	OpType string
	Args interface{}


	ClientId int64
	RequestId int64


}

func (o Op) Equal(o1 Op) bool {
	return o.OpType == o1.OpType && o.ClientId == o1.ClientId && o.RequestId == o1.RequestId
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	recvOp := Op{
		OpType:    OP_JOIN,
		Args:      args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = sm.StartOp(recvOp)

	//handle recvOp

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	recvOp := Op{
		OpType:    OP_LEAVE,
		Args:      args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = sm.StartOp(recvOp)

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	recvOp := Op{
		OpType:    OP_MOVE,
		Args:      args,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	reply.WrongLeader = sm.StartOp(recvOp)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	recvOp := Op{
		OpType:    OP_JOIN,
		Args:      args,
		ClientId:  nrand(),
		RequestId: 0,
	}

	reply.WrongLeader = sm.StartOp(recvOp)

	if !reply.WrongLeader{
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num >=0 && args.Num < len(sm.configs){
			reply.Config = sm.configs[args.Num]
		}else{
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}


}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.




}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) StartOp(originOp Op) bool  {
	wrongLeader := true

	index,_,isLeader := sm.rf.Start(originOp)

	if !isLeader{
		return true
	}

	ch := sm.getOpIndexCh(index)

	retOp := getRaftOp(ch)


	if originOp.Equal(retOp){
		wrongLeader = false
	}

	return wrongLeader
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.


	sm.ReqMap = make(map[int64]int64)
	sm.OpMap = make(map[int]chan Op)
	sm.killCh = make(chan struct{})

	go func() {
		for{
			select {
			case <- sm.killCh:
				return

			case applyMsg := <-sm.applyCh:
				if !applyMsg.CommandValid{
					continue
				}
				op := applyMsg.Command.(Op)

				sm.mu.Lock()

				clientid := op.ClientId
				reqNum,found := sm.ReqMap[clientid]

				//处理过的reqid比op的小，也就是没有收到过时的消息
				if !found || reqNum < op.RequestId {

					//handle

					sm.updateConfig(op)

					sm.ReqMap[clientid] = op.RequestId
				}



				sm.mu.Unlock()

				index := applyMsg.CommandIndex
				ch := sm.getOpIndexCh(index)
				sendCh(ch,op)




			}
		}

	}()


	return sm
}

//lock before use
func (sm *ShardMaster) updateConfig(op Op) {
	cfg := sm.getCfg()
	args := op.Args
	switch op.OpType {
	case OP_JOIN:
		joinarg := args.(JoinArgs)

		for gid, servers := range joinarg.Servers{
			server := make([]string,len(servers))
			copy(server,servers)
			cfg.Groups[gid] = server
		}

	case OP_LEAVE:
		leavearg := args.(LeaveArgs)

		for _, gid := range leavearg.GIDs{
			delete(cfg.Groups,gid)
		}

	case OP_MOVE:
		movearg := args.(MoveArgs)
		//first check arg.gid if exist
		//can not move shard to a gid that not exists
		if _, ok := cfg.Groups[movearg.GID];ok{
			cfg.Shards[movearg.Shard] = movearg.GID
		}else{
			return
		}
	}
	sm.configs = append(sm.configs,cfg)
}

func (sm *ShardMaster) getCfg() Config {

	lastcfg := sm.configs[len(sm.configs)-1]

	ret := Config{
		Num:    lastcfg.Num+1,
		Shards: lastcfg.Shards,
		Groups: make(map[int][]string),
	}

	for gid, servers := range lastcfg.Groups {
		ret.Groups[gid] = append([]string{}, servers...)
	}

	return ret
}

func (sm *ShardMaster) reSharding() {
	treemap
}