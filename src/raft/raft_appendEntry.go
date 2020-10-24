package raft

import "fmt"

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
		reply.Term = rf.currentTerm
		return
	}

	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm{
		Log().Warning.Printf("entry at prevLogIndex whose term does not matches prevLogTerm")
		reply.Term = rf.currentTerm
		return
	}

	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3) Figure 7 (f)
	//now has the same previdx and logs[previdx].term, but its term may be different


	//Append any new entries not already in the log

	oldLeaderUnCommitedIdx := -1
	Log().Info.Printf(args.String())

	for i,e:=range args.Entries{
		//i start from 0
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
		Log().Info.Printf("follower commit")
		rf.tryCommit(Min(args.LeaderCommitIdx,len(rf.logs)-1))
	}
	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//Log().Info.Printf("server %d sends append entries to server %d", rf.me,server)
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}


func (rf *Raft) broadCast() {
	Log().Info.Printf("server %d start broadcast heartbeats, rf.nextidx:%d, rf.matchidx:%d",rf.me,rf.nextIndex,rf.matchIndex)

	for i:=range rf.peers{
		if i == rf.me{
			continue
		}
		go func(peerIdx int) {
			rf.lock()
			if rf.role != Leader{
				rf.unlock()
				return
			}
			reply := AppendEntriesReply{}
			prevIdx := rf.nextIndex[peerIdx] -1
			ents := make([]LogEntry,len(rf.logs[(prevIdx+1):]))
			copy(ents,rf.logs[(prevIdx+1):])
			args := AppendEntriesArgs{
				Term:            rf.currentTerm,
				LeaderId:        rf.me,
				PrevLogIndex:    prevIdx ,
				PrevLogTerm:     rf.logs[prevIdx].Term,
				Entries: ents,
				LeaderCommitIdx: rf.commitIndex,
			}
			rf.unlock()
			ok := rf.sendAppendEntries(peerIdx,&args,&reply)
			if ok{
				rf.lock()
				defer rf.unlock()


				Log().Info.Printf(reply.String())
				//reply success
				//update nextidx and matchidx
				if reply.Success{
					rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

					//If there exists an N such that N > commitIndex, a majority
					//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					//set commitIndex = N

					for N:=len(rf.logs)-1; N>rf.commitIndex;N--{
						//Log().Info.Printf("N:%d, commitidx:%d",N,rf.commitIndex)
						cnt := 0
						for i:=range rf.matchIndex{
							if rf.matchIndex[i] >= N{
								cnt++
							}
							if cnt > len(rf.peers)/2{
								//commit
								rf.tryCommit(N)
								break
							}
						}
					}

				}else{
					//
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.changeRole(Follower)
					}else{
						//reply.term == rf.currentTerm
						//reply false, decrement nextidx
						Log().Info.Printf("heartbeat REPLY FALSE")
						rf.nextIndex[peerIdx] = args.PrevLogIndex - 1
					}

				}
			}else{
				Log().Error.Printf("server %d append entries to server %d not ok",rf.me,peerIdx)
			}
		}(i)
	}
	rf.resetHeartBeatTimer()
}