package kvraft

import (
	"bytes"
	"labgob"
	"labrpc"
	"raft"
	_ "strconv"
	"sync"
	"sync/atomic"
	"time"
)


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft

	//get rf.ApplyMsg from applyCh
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	db          map[string]string

	OpMap 		map[int]chan Op
	ReqMap		map[int64]int64

	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister
	killCh chan struct{}
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//Log().Info.Printf("get args from client")


	op := Op{
		OpType: GetCmd,
		Key:    args.Key,
		//Value: strconv.FormatInt(nrand(),10),
		Value: "",
		ClientId: args.ClientID,
		RequestId: args.RequestID,
	}
	index,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Value = ""
		reply.Msg = WrongLeader
		return
	}
	ch := kv.getOpIndexCh(index)
	//newOp := <- ch

	newOp := Op{}
	select {
	case op := <-ch:
		//Log().Info.Printf("get op")
		newOp = op
	case <- time.After(time.Duration(600)*time.Millisecond):
		//Log().Info.Printf("get timeout")
	}

	if newOp.Equal(op){
		kv.mu.Lock()
		_,found := kv.db[op.Key]
		kv.mu.Unlock()
		if !found {
			reply.Value = ""
			reply.Msg = ErrNoKey
			return
		}else{
			kv.mu.Lock()
			reply.Value = kv.db[op.Key]
			kv.mu.Unlock()
			reply.Msg = OK
			return
		}
	}else{
		reply.Msg = WrongLeader
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//Log().Debug.Printf("recv put-append args from client %d", args.ClientID)

	op := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		ClientId:  args.ClientID,
		RequestId: args.RequestID,

	}
	index,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Msg = WrongLeader
		return
	}

	ch := kv.getOpIndexCh(index)
	//newOp := <- ch
	//newOp := getOpOrTimeout(ch)
	//Log().Debug.Printf("get ch with index:%d",index)
	newOp := Op{}
	select {
	case op := <-ch:
		//Log().Info.Printf("getop")
		newOp = op
	case <- time.After(time.Duration(600)*time.Millisecond):
		//Log().Info.Printf("put timeout")
	}

	if newOp.Equal(op){
		reply.Msg = OK
		return
	}else{
		reply.Msg = WrongLeader
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.

}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getOpIndexCh(index int) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	 _,found := kv.OpMap[index]
	 if !found{
	 	kv.OpMap[index] = make(chan Op, 1)
	 }
	 return kv.OpMap[index]
}

func sendCh(ch chan Op, op Op) {
	select {
	case <-ch:
	default:
	}
	ch <- op
}

func (kv *KVServer) takeSnapshotOnDB(index int) {
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)

	kv.mu.Lock()
	_ = e.Encode(kv.db)
	_ = e.Encode(kv.ReqMap)
	kv.mu.Unlock()
	kv.rf.TakeSnapshot(index,b.Bytes())
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1{
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	b := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(b)

	var db map[string]string
	var ReqMap map[int64]int64

	if d.Decode(&db) != nil || d.Decode(&ReqMap) != nil{
		Log().Error.Printf("could not decode db && client-req map!!")
	}else{
		kv.db = db
		kv.ReqMap = ReqMap
	}
}

func (kv *KVServer) needSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//Log().Debug.Printf("raftStateSize:%d, maxRaftState:%d",kv.persister.RaftStateSize(),kv.maxraftstate)
	return kv.maxraftstate > 0 &&  kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate/10
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killCh = make(chan struct{})

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.OpMap = make(map[int]chan Op)
	kv.ReqMap = make(map[int64]int64)

	kv.restoreSnapshot(kv.persister.ReadSnapshot())

	// You may need initialization code here.
	go func() {
		for {
			select {
			case <- kv.killCh:
				return
			case msg := <- kv.applyCh:
				if !msg.CommandValid{
					Log().Info.Printf("restore snapshot")
					kv.restoreSnapshot(msg.SnapShot)
					continue
				}
				op := msg.Command.(Op)
				Log().Debug.Printf("get msg from apply ch, idx: %d, value:%s", msg.CommandIndex,op.Value)
				clientid := op.ClientId

				kv.mu.Lock()
				reqNum,found := kv.ReqMap[clientid]

				//kv.db as state machine, dont use same cmd on state machine
				if !found || op.RequestId > reqNum{
					switch op.OpType {
					case PutCmd:
						kv.db[op.Key] = op.Value
					case AppendCmd:
						kv.db[op.Key] += op.Value
					}
					kv.ReqMap[op.ClientId] = op.RequestId
				}
				kv.mu.Unlock()
				index := msg.CommandIndex
				ch := kv.getOpIndexCh(index)
				if kv.needSnapshot(){
					//Log().Debug.Printf("before log size:%d",kv.persister.RaftStateSize())
					//like redis db file saveState, new goroutine to take snapshot
					go kv.takeSnapshotOnDB(index)
					//kv.takeSnapshotOnDB(index)
					//Log().Debug.Printf("after log size:%d",kv.persister.RaftStateSize())
				}
				//Log().Debug.Printf("send ch with index:%d op:%s",index, op.Value)
				sendCh(ch,op)
			}

		}
	}()
	Log().Info.Printf("server start")
	return kv
}
