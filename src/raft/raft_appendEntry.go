package t

import "raft"

type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int

	Entries []raft.LogEntry

	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *raft.Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.lock()
	defer rf.lock()


	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.changeRole(raft.Follower)
	}else if args.Term < rf.currentTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}


	rf.resetElectionTimer()
}

func (rf *raft.Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}


func (rf *raft.Raft) broadCast() {

	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    rf.getLastLogIndex() - 1,
		PrevLogTerm:     rf.logs[rf.getLastLogIndex()-1].Term,
		Entries:         nil,
		LeaderCommitIdx: rf.commitIndex,
	}

	for i:=range rf.peers{
		go func(peerIdx int, args *AppendEntriesArgs) {
			reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(peerIdx,args,&reply)
			if ok{
				rf.lock()
				defer rf.unlock()
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.changeRole(raft.Follower)
				}
			}else{
				raft.LogInstance().Error.Printf("server %d append entries to server %d not ok",rf.me,peerIdx)
			}
		}(i,&args)
	}
}