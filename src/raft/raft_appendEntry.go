package raft

import (
	"fmt"
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm int

	Entries []LogEntry

	LeaderCommitIdx int
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("Term:%d, Leader:%d, ,LogLength:%d,PrevLogIndex:%d, PrevLogTerm:%d, LeaderCommitIdx:%d",a.Term,a.LeaderId,len(a.Entries),a.PrevLogIndex,a.PrevLogTerm,a.LeaderCommitIdx)
}

type AppendEntriesReply struct {
	Term int
	Success bool

	ConflictTerm int
	ConflictIndex int

	CommitIndex int
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.lock()
	defer rf.unlock()
	//rf.Log().Debug.Printf("server %d recv append entries from leader server %d",rf.me,args.LeaderId)

	//Log().Info.Printf("lastLogIndex:%d, argsPrevLogIndex:%d",rf.getLastLogIndex(),args.PrevLogIndex)

	//term of the conflict entry
	//reply.ConflictTerm = -1

	//index of first entry of conflictTerm
	//reply.ConflictIndex = -1
	rf.Log().Debug.Printf(rf.getPeersLogState())

	if args.Term > rf.currentTerm{
		rf.Log().Info.Printf("server %d,args.Term %d > rf.currentTerm %d",rf.me, args.Term,rf.currentTerm)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.changeRole(Follower)
	}else if args.Term < rf.currentTerm{
 		rf.Log().Warn.Printf("server %d, recv args.Term %d < rf.currentTerm %d",rf.me,args.Term,rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.CommitIndex = rf.commitIndex
		return
	}

	//Reply false if log does not contain an entry at prevLogIndex
	//shorter
	//a:4 5 6 [7]
	//b:4 5
	if rf.getLastLogIndex() < args.PrevLogIndex {
		rf.Log().Warn.Printf("have shorter log")
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.CommitIndex = rf.commitIndex
		//rf.Log().Info.Printf(rf.getPeersLogState())
		return
	}

	//whose term matches prevLogTerm (§5.3)
	//rf.lastIndex >= args.PrevIndex,
	//check lastIndex.term == logs[prevLogIndex].term
	// if false return false so that leader will let nextIndex-- to override the old logs

	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)

	//rf.lastIndex >= args.PrevIndex , may has conflicts, with the same index but different term
	//logs term: 3 4 5 5
	//logs term: 3 4 6 6 [6]

	if !rf.matchLocalTerm(args.PrevLogIndex,args.PrevLogTerm){
		rf.Log().Warn.Printf("server%d,localterm:%d does not matches prevLogTerm:%d",rf.me,rf.logs[args.PrevLogIndex].Term,args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.CommitIndex = rf.commitIndex
		//rf.Log().Info.Printf(rf.getPeersLogState())
		return
	}



	//Append any new entries not already in the log
	//logs term: 3 4 6
	//logs term: 3 4 6 [7]
	//Log().Debug.Printf("append idx from:%d",)

	//if len(rf.logs) < len()

	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs,args.Entries[:]...)

	//If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)

	if args.LeaderCommitIdx > rf.commitIndex{
		//rf.Log().Debug.Printf("server %d, leaderCommit idx:%d, rf.commitIndex:%d",rf.me,args.LeaderCommitIdx,rf.commitIndex)
		rf.tryCommit(Min(args.LeaderCommitIdx,len(rf.logs)))
	}

	reply.Success = true
	reply.CommitIndex = rf.commitIndex

	//rf.Log().Debug.Printf(rf.getPeersLogState())

	rf.resetElectionTimer()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.Log().Info.Printf("server %d sends append entries to server %d", rf.me,server)
	Log().Debug.Printf(args.String())
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}


func (rf *Raft) broadCast() {
	rf.Log().Info.Printf("server %d start broadcast heartbeats",rf.me)

	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        rf.me,
		//PrevLogIndex:    rf.getLastLogIndex() ,
		//PrevLogTerm:     rf.logs[rf.getLastLogIndex()].Term,
		Entries:         nil,
		LeaderCommitIdx: rf.commitIndex,
	}


	for i:=range rf.peers{
		if i == rf.me{
			continue
		}
		go func(peerIdx int, args *AppendEntriesArgs) {
			//Log().Info.Printf("peeridx:%d",peerIdx,)
			rf.lock()
			reply := AppendEntriesReply{}
			prevIdx:= rf.nextIndex[peerIdx] - 1
			//Log().Debug.Printf("to server %d,rf.nextIdx:%d, prevIdx:%d,",peerIdx,rf.nextIndex[peerIdx],prevIdx)
			entries := make([]LogEntry,len(rf.logs[prevIdx+1:] ))
			copy(entries,rf.logs[prevIdx+1:])
			args.Entries = entries
			args.PrevLogIndex = prevIdx
			args.PrevLogTerm = rf.getLogTerm(prevIdx)
			rf.unlock()
			rf.Log().Debug.Printf("leader %d to server %d, AppendEntriesArgs %s ,start broadcast heartbeats",rf.me, peerIdx,args.String())
			ok := rf.sendAppendEntries(peerIdx,args,&reply)
			if ok{
				rf.lock()
				defer rf.unlock()
				if reply.Term > rf.currentTerm{
					Log().Warn.Printf("server %d, found reply.Term:%d, rf.term:%d",rf.me,reply.Term,rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.changeRole(Follower)
				}

				if reply.Success{
					//follower commit args
					//If successful: update nextIndex and matchIndex for
					//follower (§5.3)
					rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1
					rf.Log().Debug.Printf("server %d, append success, rf.commitIndex:%d, matchIndex:%d, nextIndex:%d ",rf.me,rf.commitIndex,rf.matchIndex[peerIdx],rf.nextIndex[peerIdx])
					rf.Log().Debug.Printf(rf.getPeersLogState())
					//If there exists an N such that N > commitIndex, a majority
					//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					//set commitIndex = N (§5.3, §5.4).
					for N:=len(rf.logs)-1;N>rf.commitIndex;N--{
						cnt := 0
						for _,matchN := range rf.matchIndex{
							//Log().Info.Printf("N:%d, matchN:%d",N,matchN)
							if matchN >= N{
								cnt++
							}
						}

						if cnt > len(rf.peers)/2{
							rf.tryCommit(N)
							break
						}
					}



				}else{
					//If AppendEntries fails because of log inconsistency:
					//decrement nextIndex and retry (§5.3)
					rf.Log().Warn.Printf("server %d,append failed, try decrement to reply.commitIndex:%d ",rf.me,reply.CommitIndex)
					//rf.nextIndex[peerIdx] = reply.CommitIndex + 1
					rf.nextIndex[peerIdx] -= 1
				}

			}else{
				//rf.Log().Error.Printf("server %d append entries to server %d not ok",rf.me,peerIdx)
			}
		}(i,&args)
	}
	rf.resetHeartBeatTimer()
}

func (rf *Raft) matchLocalTerm(localIdx, term int) bool {
	t := rf.logs[localIdx].Term
	return t == term
}


func (rf *Raft) findConflict(argEnts []LogEntry, argsPrevIdx int) int {

	for idx,ne := range argEnts {
		//todo: can PrevIdx instead of ne.index?
		if (len(rf.logs) < (argsPrevIdx+2+idx)) || !rf.matchLocalTerm(argsPrevIdx+1+idx, ne.Term){
			return idx
		}

	}
	//no conflict entries
	return -1
}

func (rf *Raft) tryCommit(commitIdx int) {
	rf.commitIndex = commitIdx
	//rf.Log().Info.Printf("try commit from : rf.commitIndex:%d,rf.lastApplied:%d",rf.commitIndex,rf.lastApplied)
	if rf.commitIndex > rf.lastApplied{
		entriesToApply := append([]LogEntry{},rf.logs[rf.lastApplied+1:rf.commitIndex+1]...)

		go func(start int,entries []LogEntry) {
			for idx,entry := range entries{
				var msg ApplyMsg
				msg.Command = entry.Cmd
				msg.CommandIndex = start + idx
				msg.CommandValid = true
				rf.applyCh <- msg
				rf.lock()
				if msg.CommandIndex > rf.lastApplied{
					rf.lastApplied = msg.CommandIndex
				}
				rf.unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}