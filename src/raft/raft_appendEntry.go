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
	defer rf.sendCh(rf.heartbeatCh)
	defer rf.persist()

	//Log().Debug.Printf("server %d recv append entries from leader server %d",rf.me,args.LeaderId)



	if args.Term > rf.currentTerm{
		//Log().Info.Printf("args.Term %d > rf.currentTerm %d",args.Term,rf.currentTerm)
		rf.changeToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm{
		//Log().Warning.Printf("args.Term %d < rf.currentTerm %d",args.Term,rf.currentTerm)
		return
	}


	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)

	//shorter log, conflict idx = len(rf.logs)

	//如果一个follower在日志中没有prevLogIndex，它应该返回conflictIndex=len(log)和conflictTerm = None。
	prevLogTerm := -1
	if args.PrevLogIndex >= rf.lastIncludeIndex && args.PrevLogIndex <= rf.getLastLogIndex() {
		prevLogTerm = rf.getTerm(args.PrevLogIndex)
	}

	logLength := rf.getLogsLength()


	if prevLogTerm != args.PrevLogTerm{
		reply.ConflictIndex = logLength
		//如果一个follower在日志中没有prevLogIndex，它应该返回conflictIndex=len(log)和conflictTerm = None。
		if prevLogTerm == -1{
			return
		}else{
			//如果一个follower在日志中有prevLogIndex，但是term不匹配，
			//它应该返回conflictTerm = log[prevLogIndex].Term，
			//并且查询它的日志以找到第一个term等于conflictTerm的条目的index。
			reply.ConflictTerm = prevLogTerm
			for i:= rf.lastIncludeIndex; i<logLength; i++{
				if rf.getTerm(i) == prevLogTerm{
					reply.ConflictIndex = i
					break
				}
			}
			return
		}
	}

	//now has the same previdx and logs[previdx].term, but its term may be different
	//Append any new entries not already in the log
	idx := args.PrevLogIndex
	for i,e := range args.Entries{
		idx++
		if idx < logLength{
			if rf.getLog(idx).Term == e.Term{
				continue
			}else{
				//If an existing entry conflicts with a new one (same index
				//but different terms), delete the existing entry and all that
				//follow it (§5.3) Figure 7 (f)
				rf.logs = rf.logs[:idx - rf.lastIncludeIndex]
			}
		}
		rf.logs = append(rf.logs,args.Entries[i:]...)
		break
	}

	//If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)

	if rf.commitIndex < args.LeaderCommitIdx{
		//Log().Debug.Printf("follower commit, rf.commit:%d, arg.leadercommit:%d",rf.commitIndex,args.LeaderCommitIdx)
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
	//Log().Info.Printf("server %d start broadcast heartbeats, rf.nextidx:%d, rf.matchidx:%d",rf.me,rf.nextIndex,rf.matchIndex)

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

	if rf.role != Leader {
		rf.unlock()
		return
	}
	//Log().Debug.Printf("nextIndex[%d]:%d, lastIncludeIdx:%d",peerIdx,rf.nextIndex[peerIdx],rf.lastIncludeIndex)
	//如果要发送给follower的日志已经被压缩了，就直接发送快照
	if rf.nextIndex[peerIdx] - rf.lastIncludeIndex < 1{
		//Log().Debug.Printf("send install snapshot to %d",peerIdx)
		rf.unlock()
		rf.InstallSnapshot(peerIdx)
		return
	}

	prevLogIndex := rf.getPrevLogIndex(peerIdx)
	//Log().Debug.Printf("append log prevLogIdx:%d, lastIncludeIdx:%d",prevLogIndex, rf.lastIncludeIndex)
	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		PrevLogIndex:    prevLogIndex,
		PrevLogTerm:     rf.getPrevLogTerm(peerIdx),
		Entries:         append(make([]LogEntry, 0), rf.logs[rf.nextIndex[peerIdx]-rf.lastIncludeIndex:]...),
		LeaderCommitIdx: rf.commitIndex,
	}
	rf.unlock()
	//Log().Debug.Printf("prevLogIdx:%d, argLogLen:%d",prevLogIndex,len(args.Entries))
	//Log().Debug.Printf("send hb to %d",peerIdx)

	ok := rf.sendAppendEntries(peerIdx, &args, &reply)
	if ok {

		rf.lock()
		defer rf.unlock()

		//From experience, we have found that by far the simplest thing to do is to first record the term in the reply
		//(it may be higher than your current term), and then to compare the current term with the term you sent in your original RPC.
		//If the two are different, drop the reply and return.
		//Only if the two terms are the same should you continue processing the reply.

		if rf.role != Leader || rf.currentTerm != args.Term {
			return
		}

		//If RPC request or response contains term T > currentTerm:
		//set currentTerm = T, convert to follower (§5.1)
		if reply.Term > args.Term {
			rf.changeToFollower(reply.Term)
			return
		}

		if reply.Success {
			//If successful: update nextIndex and matchIndex for
			//follower (§5.3)
			rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1
			rf.updateCommitIdx()
			return
		} else {

			//If AppendEntries fails because of log inconsistency:
			//decrement nextIndex and retry (§5.3)

			//一旦收到一个冲突的响应，leader应该首先查询其日志以找到conflictTerm。
			//如果它发现日志中的一个具有该term的条目，它应该设置nextIndex为日志中该term内的最后一个条目的索引后面的一个。
			foundConflictTerm := false
			if reply.ConflictTerm != -1 {
				for i := args.PrevLogIndex; i >= 1; i-- {
					if rf.getTerm(i) == reply.ConflictTerm {
						rf.nextIndex[peerIdx] = i + 1
						foundConflictTerm = true
						break
					}
				}
			}
			//如果它没有找到该term内的一个条目，它应该设置nextIndex=conflictIndex。

			if !foundConflictTerm {
				//leader may take snapshot, and follower reply.conflict index may less than logs length
				rf.nextIndex[peerIdx] = Min(reply.ConflictIndex,rf.getLogsLength())
			}
		}
	}
}

//lock before calling
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

	if N > rf.commitIndex && rf.getTerm(N) == rf.currentTerm{
		rf.commitIndex = N
		//Log().Debug.Printf("server %d commit index update to %d",rf.me,tmp[n])
		//rf.startApply <- 1
		rf.updateLastApplied()

	}
}

//lock before calling
func (rf *Raft) updateLastApplied() {

	rf.lastApplied = Max(rf.lastIncludeIndex,rf.lastApplied)
	rf.commitIndex = Max(rf.lastIncludeIndex,rf.commitIndex)

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		Log().Debug.Printf("lastApplied:%d, lastIncludeIdx:%d",rf.lastApplied,rf.lastIncludeIndex)
		curLog := rf.getLog(rf.lastApplied)
		applyMsg := ApplyMsg{
			true,
			curLog.Cmd,
			rf.lastApplied,
			nil,
		}
		rf.applyCh <- applyMsg
	}

}

