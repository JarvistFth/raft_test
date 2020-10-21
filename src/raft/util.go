package raft

import (
	"log"
	"os"
	"sync"
)

type Log struct {
	Debug   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
}

var once sync.Once

var L *Log

func LogInstance() *Log {
	once.Do(func() {
		L = &Log{
			Debug:   log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime|log.Lshortfile),
			Info:    log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime),
			Warning: log.New(os.Stdout, "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile),
			Error:   log.New(os.Stdout, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile),
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
