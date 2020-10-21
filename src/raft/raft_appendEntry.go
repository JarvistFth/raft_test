package raft

type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int

	Entries []LogEntry

	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.lock()
	defer rf.unlock()
	LogInstance().Debug.Printf("server %d recv append entries from leader server %d",rf.me,args.LeaderId)

	if args.Term > rf.currentTerm{
		LogInstance().Info.Printf("args.Term %d > rf.currentTerm %d",args.Term,rf.currentTerm)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.changeRole(Follower)
	}else if args.Term < rf.currentTerm{
		LogInstance().Warning.Printf("args.Term %d < rf.currentTerm %d",args.Term,rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}


	rf.resetElectionTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	LogInstance().Info.Printf("server %d sends append entries to server %d", rf.me,server)
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}


func (rf *Raft) broadCast() {
	LogInstance().Info.Printf("server %d start broadcast heartbeats",rf.me)
	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    rf.getLastLogIndex() ,
		PrevLogTerm:     rf.logs[rf.getLastLogIndex()].Term,
		Entries:         nil,
		LeaderCommitIdx: rf.commitIndex,
	}

	for i:=range rf.peers{
		if i == rf.me{
			continue
		}
		go func(peerIdx int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(peerIdx,args,&reply)
			if ok{
				rf.lock()
				defer rf.unlock()
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.changeRole(Follower)
				}
			}else{
				LogInstance().Error.Printf("server %d append entries to server %d not ok",rf.me,peerIdx)
			}
		}(i,&args)
	}
	rf.resetHeartBeatTimer()
}