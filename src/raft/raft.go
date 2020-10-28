package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)



type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Cmd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role        Role
	currentTerm int

	votesGranted int
	voteFor int

	logs    []LogEntry
	applyCh chan ApplyMsg

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	electionTimer *time.Timer
	heartBeatTimer *time.Timer




	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock()
	defer rf.unlock()

	term = rf.currentTerm
	isleader = rf.role == Leader
	Log().Info.Printf("server %d, term:%d, role:%s",rf.me,rf.currentTerm,rf.role.String())
	return term, isleader
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.lock()
	defer rf.unlock()
	term = rf.currentTerm
	isLeader = rf.role == Leader
	if isLeader{
		rf.logs = append(rf.logs,LogEntry{
			Term:  rf.currentTerm,
			Cmd:   command,
		})
		index = len(rf.logs) - 1
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		//Log().Info.Printf("get cmd from client, logslength:%d",len(rf.logs))
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.voteFor = -1
	rf.votesGranted = 0
	rf.role = Follower
	rf.lastApplied = 0

	rf.logs = make([]LogEntry,1)


	rf.nextIndex = make([]int,len(rf.peers))


	rf.matchIndex = make([]int,len(rf.peers))

	rf.electionTimer = time.NewTimer(rf.getRandomDuration())
	rf.heartBeatTimer = time.NewTimer(heartBeatTimeout)

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func(rf *Raft) {
		for{
			select {
			case <- rf.electionTimer.C:
				rf.lock()
				role := rf.role
				switch role {
				case Follower:
					rf.changeRole(Candidate)
				case Candidate:
					rf.startElection()
				}
				rf.unlock()
			case <- rf.heartBeatTimer.C:
				rf.lock()
				role := rf.role
				if role == Leader {
					rf.resetHeartBeatTimer()
					rf.broadCast()
				}
				rf.unlock()
			}
		}
	}(rf)

	go func() {
		for !rf.killed(){
			time.Sleep(applyTimeout)
			rf.lock()
			if rf.lastApplied < rf.commitIndex{
				Log().Info.Printf("apply,index:%d",rf.lastApplied)
				for rf.lastApplied< rf.commitIndex{
					rf.lastApplied++
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.logs[rf.lastApplied].Cmd,
						CommandIndex: rf.lastApplied,
					}
					applyCh <- applyMsg
				}
			}
			rf.unlock()
		}
	}()

	return rf
}

//need lock before using
func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
		Log().Info.Printf("server %d change to follower at term %d",rf.me,rf.currentTerm)
		rf.heartBeatTimer.Stop()
		rf.resetElectionTimer()
		rf.voteFor = -1
	case Candidate:
		Log().Info.Printf("server %d change to Candidate at term %d",rf.me,rf.currentTerm)
		rf.startElection()
	case Leader:
		Log().Info.Printf("server %d change to Leader at term %d",rf.me,rf.currentTerm)
		rf.electionTimer.Stop()
		for i := range rf.nextIndex{
			rf.nextIndex[i] = len(rf.logs)
		}

		for i:= range rf.matchIndex{
			rf.matchIndex[i] = 0
		}
		rf.resetHeartBeatTimer()
		rf.broadCast()
	}

}

func (rf*Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) getRandomDuration() time.Duration {
	r := time.Duration(rand.Int63()) % electionTimeout
	return electionTimeout + r
	//return time.Duration(heartBeatTimeout*3+rand.Intn(heartBeatTimeout))*time.Millisecond
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.getRandomDuration())
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(heartBeatTimeout)
}

func (rf *Raft) getLastLogIndex() int{
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	back := len(rf.logs) - 1
	return rf.logs[back].Term
}

func (rf *Raft) getTerm(idx int) int {
	return rf.logs[idx].Term
}

//func (rf *Raft) tryCommit(commitIdx int) {
//	rf.commitIndex = commitIdx
//
//	if rf.commitIndex > rf.lastApplied{
//
//		entsToApply := append([]LogEntry{},rf.logs[(rf.lastApplied+1):(rf.commitIndex+1)]...)
//		Log().Debug.Printf("server %d try to apply from, last applied idx:%d, to commitidx:%d, entslen:%d",rf.me,rf.lastApplied,rf.commitIndex,len(entsToApply))
//
//		go func(start int,entries []LogEntry) {
//
//			for idx,ent := range entries{
//
//				apMsg := ApplyMsg{
//					CommandValid: true,
//					Command:      ent.Cmd,
//					CommandIndex: start + idx,
//				}
//				rf.applyCh <- apMsg
//
//				rf.lock()
//				if apMsg.CommandIndex > rf.lastApplied{
//					rf.lastApplied = apMsg.CommandIndex
//				}
//				rf.unlock()
//			}
//
//
//		}(rf.lastApplied+1,entsToApply)
//	}
//}
