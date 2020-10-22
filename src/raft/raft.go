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
	Index int
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
	Log().Info.Printf("server %d, term:%d, role:%s",rf.me,rf.currentTerm,roleToString(rf.role))
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
			//Index: rf.getLastLogIndex() + 1,
			Cmd:   command,
		})
		index = len(rf.logs) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		Log().Info.Printf("get cmd from client, rf.logs.length:%d",len(rf.logs))
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

	rf.logs = make([]LogEntry,1)


	rf.nextIndex = make([]int,len(rf.peers))
	for i := range rf.nextIndex{
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int,len(rf.peers))
	for i := range rf.matchIndex{
		rf.matchIndex[i] = 0
	}



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
					rf.broadCast()

				}
				rf.unlock()
			}
		}
	}(rf)

	return rf
}

//need lock before using
func (rf *Raft) changeRole(role Role) {
	rf.role = role
	Log().Debug.Printf(rf.getPeersLogState())
	switch role {
	case Follower:
		Log().Info.Printf("server %d change to follower at term %d",rf.me,rf.currentTerm)
		rf.voteFor = -1
		rf.heartBeatTimer.Stop()
		rf.resetElectionTimer()
	case Candidate:
		Log().Info.Printf("server %d change to Candidate at term %d",rf.me,rf.currentTerm)
		rf.resetElectionTimer()
		rf.startElection()
	case Leader:
		Log().Info.Printf("server %d change to Leader at term %d",rf.me,rf.currentTerm)
		for i := range rf.nextIndex{
			rf.nextIndex[i] = len(rf.logs)
		}
		for i := range rf.matchIndex{
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
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

func (rf *Raft) getLogTerm(idx int) int {
	return rf.logs[idx].Term
}
