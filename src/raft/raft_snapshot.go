package raft

type InstallSnapshotArgs struct {
	Term int
	LeaderId int

	LastIncludeIndex int //snapshot replace all and include this index logs
	LastIncludeTerm int //term of lastIncludeIndex's log

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) TakeSnapshot(index int, snapshot []byte)  {
	rf.lock()
	defer rf.unlock()
	if index <= rf.lastIncludeIndex{
		return
	}
	Log().Debug.Printf("SaveStateAndSnapshot, lastIncludeIndex:%d",rf.lastIncludeIndex)

	rf.logs = append(make([]LogEntry,0),rf.logs[index - rf.lastIncludeIndex:]...)
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.getTerm(index)
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(),snapshot)
}

func (rf *Raft) SendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestInstallSnapshot",args,reply)
	return ok
}

func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock()
	defer rf.unlock()
	Log().Debug.Printf("follower %d snapshot",rf.me)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm{
		return
	}

	if args.Term > rf.currentTerm{
		rf.changeToFollower(args.Term)
	}
	rf.sendCh(rf.heartbeatCh)

	if args.LastIncludeIndex <= rf.lastIncludeIndex{
		return
	}
	Log().Debug.Printf("follower snapshot")

	if args.LastIncludeIndex >= rf.getLogsLength() - 1{
		//discard all the logs
		rf.logs = []LogEntry{{
			Term: args.LastIncludeTerm,
			Cmd:  nil,
		}}
	}else{
		rf.logs	= append(make([]LogEntry,0),rf.logs[args.LastIncludeIndex - rf.lastIncludeIndex:]...)
	}

	//update lastIncludeIndex & terms
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm

	//update commit idx & last applied idx



	//saveStateWithSnapshot
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(),args.Data)

	rf.lastApplied = args.LastIncludeIndex
	rf.commitIndex = args.LastIncludeIndex

	if rf.lastApplied > rf.lastIncludeIndex{
		return
	}
	//reset state machine using snapshot content
	applyMsg := ApplyMsg{
		CommandValid: false,
		SnapShot:     args.Data,
	}
	rf.applyCh <- applyMsg


}

func (rf *Raft) InstallSnapshot(peerIdx int) {
	rf.lock()
	//Log().Debug.Printf("install snapshot to %d",peerIdx)

	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	rf.unlock()


	reply := InstallSnapshotReply{}
	ok := rf.SendInstallSnapshotRPC(peerIdx,&args,&reply)
	if ok{
		rf.lock()
		defer rf.unlock()
		if rf.currentTerm != args.Term || rf.role != Leader{
			return
		}
		if reply.Term > rf.currentTerm{
			rf.changeToFollower(reply.Term)
			return
		}
		//update match index with included index
		rf.matchIndex[peerIdx] = rf.lastIncludeIndex
		rf.nextIndex[peerIdx] = rf.lastIncludeIndex + 1
		rf.updateCommitIdx()
	}else{
		//Log().Error.Printf("install snapshot failed")
	}
}



