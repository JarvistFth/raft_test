package raft

import (
	"bytes"
	"labgob"
)

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

	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.voteFor)
	_ = e.Encode(rf.logs)
	data := b.Bytes()
	rf.persister.SaveRaftState(data)
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

	b := bytes.NewBuffer(data)
	d := labgob.NewDecoder(b)

	var logs []LogEntry
	var currentTerm int
	var voteFor int

	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor)!=nil || d.Decode(&logs)!=nil{
		//Log().Error.Printf("read state decoding error!!")
		return
	}
	rf.lock()
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logs = logs
	rf.unlock()
}
