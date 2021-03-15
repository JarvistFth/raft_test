package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//log.Infof("recv GetArgs from client:%d, reqid:%d",args.ClientID,args.RequestID)

	//kv.mu.Lock()
	//for k,v := range kv.db{
	//	log.Debugf("key:%v, value:%v",k,v)
	//}
	//kv.mu.Unlock()

	op := Op{
		OpType:    OP_GET,
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientID,
		RequestId: args.RequestID,
	}

	index,_, isLeader := kv.rf.Start(op)

	if !isLeader{
		reply.Msg = ErrWrongLeader
		return
	}

	ch := kv.getOpIndexCh(index,true)
	retop := getRaftOp(ch)

	//log.Debugf("retop:%s, argsop:%s",retop.String(),op.String())

	if retop.Equal(op){
		reply.Msg = OK
		reply.Value = retop.Value
		return
	}else if retop.OpType == ErrWrongGroup {
		reply.Msg = ErrWrongGroup
		return
	}else if retop.OpType == ErrNoKey{
		reply.Msg = ErrNoKey
		return
	} else{
		reply.Msg = ErrTimeout
		return
	}


}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	//log.Infof("recv PutAppendArgs from client:%d reqid:%d, key:%v, value:%v",args.ClientID, args.RequestID,args.Key,args.Value)
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientID,
		RequestId: args.RequestID,
	}

	index,_,isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Msg = ErrWrongLeader
		return
	}

	ch := kv.getOpIndexCh(index,true)
	retop := getRaftOp(ch)

	if retop.Equal(op){
		reply.Msg = OK
		return
	}else if retop.OpType == ErrWrongGroup {
		log.Debugf("retop:%s, argsop:%s",retop.String(),op.String())
		reply.Msg = ErrWrongGroup
		return
	} else{
		reply.Msg = ErrTimeout
		return
	}
}
