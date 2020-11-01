package raft

import (
	"math/rand"
	"time"
)

const(
	heartBeatTimeout = 100 * time.Millisecond
	electionTimeout = 300 * time.Millisecond
	applyTimeout =  150 * time.Millisecond

)

func (rf*Raft) lock() {
	//can use Log before locking
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	//can use unlock before unlocking
	rf.mu.Unlock()
}

func (rf *Raft) getRandomDuration() time.Duration {
	return time.Duration(rand.Intn(200) + 300) * time.Millisecond
	//return time.Duration(heartBeatTimeout*3+rand.Intn(heartBeatTimeout))*time.Millisecond
}

func (rf *Raft) sendCh(ch chan struct{}) {
	select {
	case <-ch:
		//break
	default:
	}
	ch <- struct{}{}
}


//reset timers
//func (rf *Raft) resetElectionTimer() {
//	rf.electionTimer.Stop()
//	rf.electionTimer.Reset(rf.getRandomDuration())
//}
//
//func (rf *Raft) resetHeartBeatTimer() {
//	rf.heartBeatTimer.Stop()
//	rf.heartBeatTimer.Reset(heartBeatTimeout)
//}


// get some log index or terms
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