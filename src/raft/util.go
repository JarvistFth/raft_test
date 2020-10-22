package raft

import (
	"fmt"
	"log"
	"os"
	"sync"
)

type Logger struct {
	Debug *log.Logger
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
}

var once sync.Once

var L *Logger

func (rf *Raft)Log() *Logger {
	once.Do(func() {
		L = &Logger{
			Debug: log.New(os.Stdout, "[DEBUG] " + fmt.Sprintf("server:%d",rf.me), log.Ltime|log.Lshortfile),
			Info:  log.New(os.Stdout, "[INFO] " + fmt.Sprintf("server:%d",rf.me), log.Ltime),
			Warn:  log.New(os.Stdout, "[WARN] " + fmt.Sprintf("server:%d",rf.me), log.Ltime|log.Lshortfile),
			Error: log.New(os.Stdout, "[ERROR] "  + fmt.Sprintf("server:%d",rf.me), log.Ltime|log.Lshortfile),
		}
	})
	return L
}

func Log() *Logger {
	once.Do(func() {
		L = &Logger{
			Debug: log.New(os.Stdout, "[DEBUG] " , log.Ltime|log.Lshortfile),
			Info:  log.New(os.Stdout, "[INFO] " , log.Ltime),
			Warn:  log.New(os.Stdout, "[WARN] " , log.Ltime|log.Lshortfile),
			Error: log.New(os.Stdout, "[ERROR] " , log.Ltime|log.Lshortfile),
		}
	})
	return L
}

func Min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

func roleToString(role Role) string {
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

func (rf *Raft) getPeersLogState() string{
	return fmt.Sprintf("server:%d, role:%s, term:%d,LogsLength:%d, rf.commit:%d, ,rf.lastApplied:%d, rf.nextIndex:%d, rf.matchidx:%d,",
		rf.me,roleToString(rf.role), rf.currentTerm, len(rf.logs),rf.commitIndex,rf.lastApplied,rf.nextIndex, rf.matchIndex)
}

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)

	}
	return
}


