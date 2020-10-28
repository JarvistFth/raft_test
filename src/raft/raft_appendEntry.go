package raft

import (
	"fmt"
	"sort"
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int

	Entries []LogEntry

	LeaderCommitIdx int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("term:%d, leaderid:%d, prevIndex:%d, prevTerm:%d, leaderCommitIdx:%d",
		args.Term,args.LeaderId,args.PrevLogIndex,args.PrevLogTerm,args.LeaderCommitIdx)
}

type AppendEntriesReply struct {
	Term int
	Success bool

	ConflictIndex int
	ConflictTerm int
}

func (reply AppendEntriesReply) String() string {
	if reply.Success{
		return fmt.Sprintf("reply.term %d, success:%s",reply.Term,"true")
	}
	return fmt.Sprintf("reply.term %d, reply.success:%s",reply.Term,"false")
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.lock()
	defer rf.unlock()
	defer rf.persist()
	Log().Debug.Printf("server %d recv append entries from leader server %d",rf.me,args.LeaderId)

	reply.Success = false

	if args.Term < rf.currentTerm{
		Log().Warning.Printf("args.Term %d < rf.currentTerm %d",args.Term,rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}else if args.Term > rf.currentTerm{
		Log().Info.Printf("args.Term %d > rf.currentTerm %d",args.Term,rf.currentTerm)
		rf.currentTerm = args.Term
		rf.changeRole(Follower)
	}

	rf.resetElectionTimer()

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	if rf.getLastLogIndex() < args.PrevLogIndex {
		Log().Warning.Printf("missing log")
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		reply.Term = rf.currentTerm
		return
	}

	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm{
		Log().Warning.Printf("entry at prevLogIndex whose term does not matches prevLogTerm")

		//contain logs that leader doesn't have
		reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)
		conflictidx := args.PrevLogIndex

		for rf.getTerm(conflictidx - 1) == reply.ConflictTerm{
			conflictidx --
		}
		reply.ConflictIndex = conflictidx
		reply.Term = rf.currentTerm
		return
	}

	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3) Figure 7 (f)
	//now has the same previdx and logs[previdx].term, but its term may be different


	//Append any new entries not already in the log

	oldLeaderUnCommitedIdx := -1
	//Log().Info.Printf(args.String())

	for i,e:=range args.Entries{
		//i start from 0
		//append new || same index but different term
		if rf.getLastLogIndex() < args.PrevLogIndex+1+i || e.Term != rf.logs[(args.PrevLogIndex+i+1)].Term {
			oldLeaderUnCommitedIdx = i
			break
		}
	}

	if oldLeaderUnCommitedIdx != -1{
		rf.logs = rf.logs[:args.PrevLogIndex + oldLeaderUnCommitedIdx+1]
		rf.logs = append(rf.logs,args.Entries[oldLeaderUnCommitedIdx:]...)
	}

	//If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)

	if rf.commitIndex < args.LeaderCommitIdx{
		Log().Debug.Printf("follower commit, rf.commit:%d, arg.leadercommit:%d",rf.commitIndex,args.LeaderCommitIdx)
		//rf.tryCommit(Min(args.LeaderCommitIdx,len(rf.logs)-1))
		rf.commitIndex = Min(args.LeaderCommitIdx,rf.getLastLogIndex())
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//Log().Info.Printf("server %d sends append entries to server %d", rf.me,server)
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

//lock before using
func (rf *Raft) broadCast() {
	Log().Info.Printf("server %d start broadcast heartbeats, rf.nextidx:%d, rf.matchidx:%d",rf.me,rf.nextIndex,rf.matchIndex)

	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    0,
		PrevLogTerm:     0,
		Entries:         nil,
		LeaderCommitIdx: rf.commitIndex,
	}

	for i:=range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesRPC(i,&args)
	}
	rf.resetHeartBeatTimer()
}

func (rf *Raft) sendAppendEntriesRPC(peerIdx int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}

	rf.lock()

	if rf.role != Leader{
		return
	}

	prevLogIndex := rf.nextIndex[peerIdx] - 1
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.getTerm(prevLogIndex)
	args.Entries = make([]LogEntry,len(rf.logs[prevLogIndex+1:]))
	copy(args.Entries,rf.logs[prevLogIndex+1:])
	rf.unlock()

	ok := rf.peers[peerIdx].Call("Raft.AppendEntries",&args,&reply)
	if ok{
		rf.lock()
		defer rf.unlock()
		if rf.role != Leader{
			return
		}

		if reply.Success{

		}else{
			if reply.Term > args.Term{
				rf.currentTerm = reply.Term
				rf.changeRole(Follower)
				return
			}else{

			}
		}

	}else{
		//fail
	}



}

func (rf *Raft) leaderCommitN() {
	rf.matchIndex[rf.me] = rf.getLastLogIndex()

	tmp := make([]int, len(rf.matchIndex))
	copy(tmp,rf.matchIndex)
	sort.Ints(tmp)
	n := (len(rf.peers) - 1) / 2
	N := tmp[n]

	if N > rf.commitIndex && rf.logs[N].Term == rf.currentTerm{
		rf.commitIndex = tmp[n]
		Log().Debug.Printf("server %d commit index update to %d",rf.me,tmp[n])
	}
}