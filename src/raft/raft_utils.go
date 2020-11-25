package raft

import (
	"bytes"
	"labgob"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const(
	heartBeatTimeout = 100 * time.Millisecond
	electionTimeout = 300 * time.Millisecond
	applyTimeout =  150 * time.Millisecond

)


type Logger struct {
	Debug   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
}

var once sync.Once

var L *Logger

func Log() *Logger {
	once.Do(func() {
		L = &Logger{
			Debug:   log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime|log.Lshortfile),
			Info:    log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime),
			Warning: log.New(os.Stdout, "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile),
			Error:   log.New(os.Stdout, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile),
		}
	})
	return L
}


func (role Role) String() string {
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "unknown role"
	}
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		//_,file,line,ok := runtime.Caller(1)
		//if(!ok){
		//
		//}
		log.Printf(format, a...)

	}
	return
}



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




// get some log index or terms
func (rf *Raft) getLastLogIndex() int{
	return rf.getLogsLength() - 1
}

func (rf *Raft) getLastLogTerm() int {
	back := rf.getLastLogIndex()
	return rf.getTerm(back)
}

func (rf *Raft) getPrevLogIndex(peeridx int) int {
	return rf.nextIndex[peeridx] - 1
}

func (rf *Raft) getPrevLogTerm(peeridx int) int {
	previdx := rf.getPrevLogIndex(peeridx)
	return rf.getTerm(previdx)
}

func (rf *Raft) getTerm(idx int) int {
	if idx < rf.lastIncludeIndex{
		return -1
	}
	return rf.getLog(idx).Term
}

func (rf *Raft) getLog(idx int) LogEntry {
	return rf.logs[idx - rf.lastIncludeIndex]
}

func (rf *Raft) getLogsLength() int {
	return len(rf.logs) + rf.lastIncludeIndex
}

//lock before
func (rf *Raft) encodeRaftState() []byte {
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.voteFor)
	_ = e.Encode(rf.logs)
	_ = e.Encode(rf.lastIncludeIndex)
	_ = e.Encode(rf.lastIncludeTerm)
	return b.Bytes()
}

func Min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func Max(x,y int) int{
	if x > y{
		return x
	}else {
		return y
	}
}

