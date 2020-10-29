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

	if args.Term > rf.currentTerm{
		Log().Info.Printf("args.Term %d > rf.currentTerm %d",args.Term,rf.currentTerm)
		rf.currentTerm = args.Term
		rf.changeRole(Follower)
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm{
		Log().Warning.Printf("args.Term %d < rf.currentTerm %d",args.Term,rf.currentTerm)
		return
	}

	defer rf.resetElectionTimer()

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)

	//shorter log, conflict idx = len(rf.logs)

	//如果一个follower在日志中没有prevLogIndex，它应该返回conflictIndex=len(log)和conflictTerm = None。
	if rf.getLastLogIndex() < args.PrevLogIndex {
		Log().Warning.Printf("missing log")
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	//如果一个follower在日志中有prevLogIndex，但是term不匹配，
	//它应该返回conflictTerm = log[prevLogIndex].Term，
	//并且查询它的日志以找到第一个term等于conflictTerm的条目的index。
	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm{
		Log().Warning.Printf("entry at prevLogIndex whose term does not matches prevLogTerm")

		//contain logs that leader doesn't have
		reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)
		conflictidx := args.PrevLogIndex

		for rf.getTerm(conflictidx - 1) == reply.ConflictTerm{
			conflictidx --
		}
		reply.ConflictIndex = conflictidx
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
		//rf.startApply <- 1
		rf.updateLastApplied()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//Log().Info.Printf("server %d sends append entries to server %d", rf.me,server)
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

//lock before using
func (rf *Raft) broadCast() {
	Log().Info.Printf("server %d start broadcast heartbeats, rf.nextidx:%d, rf.matchidx:%d",rf.me,rf.nextIndex,rf.matchIndex)

	for i:=range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesRPC(i)
	}
}

func (rf *Raft) sendAppendEntriesRPC(peerIdx int) {
	reply := AppendEntriesReply{}

	rf.lock()

	if rf.role != Leader{
		return
	}

	prevLogIndex := rf.nextIndex[peerIdx] - 1

	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    prevLogIndex,
		PrevLogTerm:     rf.getTerm(prevLogIndex),
		Entries:         nil,
		LeaderCommitIdx: rf.commitIndex,
	}
	args.Entries = make([]LogEntry,len(rf.logs[prevLogIndex+1:]))
	copy(args.Entries,rf.logs[prevLogIndex+1:])
	rf.unlock()
	//Log().Debug.Printf("prevLogIdx:%d, argLogLen:%d",prevLogIndex,len(args.Entries))

	ok := rf.sendAppendEntries(peerIdx,&args,&reply)
	if ok{

		rf.lock()
		defer rf.unlock()
		if rf.role != Leader || rf.currentTerm != args.Term{
			return
		}

		if reply.Term > args.Term{
			rf.currentTerm = reply.Term
			rf.changeRole(Follower)
			rf.persist()
			return
		}

		if reply.Success{
			//If successful: update nextIndex and matchIndex for
			//follower (§5.3)
			rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1
			rf.updateCommitIdx()
		}else {

				//If AppendEntries fails because of log inconsistency:
				//decrement nextIndex and retry (§5.3)

				//一旦收到一个冲突的响应，leader应该首先查询其日志以找到conflictTerm。
				//如果它发现日志中的一个具有该term的条目，它应该设置nextIndex为日志中该term内的最后一个条目的索引后面的一个。
				foundConflictTerm := false
				if reply.ConflictTerm != -1{
					for i:= args.PrevLogIndex ; i >= 1; i--{
						if rf.getTerm(i) == reply.ConflictTerm{
							rf.nextIndex[peerIdx] = i+1
							foundConflictTerm = true
							break
						}
					}
				}
				//如果它没有找到该term内的一个条目，它应该设置nextIndex=conflictIndex。

				if !foundConflictTerm{
					rf.nextIndex[peerIdx] = reply.ConflictIndex
				}
		}
	}

}

func (rf *Raft) updateCommitIdx() {
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp,rf.matchIndex)
	sort.Ints(tmp)
	n := (len(rf.peers) - 1) / 2
	N := tmp[n]

	if N > rf.commitIndex && rf.logs[N].Term == rf.currentTerm{
		rf.commitIndex = N
		Log().Debug.Printf("server %d commit index update to %d",rf.me,tmp[n])
		//rf.startApply <- 1
		rf.updateLastApplied()

	}
}

func (rf *Raft) updateLastApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.logs[rf.lastApplied]
		applyMsg := ApplyMsg{
			true,
			curLog.Cmd,
			rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

