package raft

func (rf *Raft) changeToCandidate() {

	//On conversion to candidate, start election:
	//• Increment currentTerm
	//• Vote for self
	//• Reset election timer
	//• Send RequestVote RPCs to all other servers

	rf.role = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.votesGranted = 1
	rf.persist()
	//Log().Debug.Printf("server %d, change to candidate at term:%d",rf.me,rf.currentTerm)

	go rf.startElection()
}

func (rf *Raft) changeToFollower(term int) {
	//defer Log().Debug.Printf("server %d, change to follower at term:%d",rf.me,rf.currentTerm)
	rf.role = Follower
	rf.voteFor = -1
	rf.votesGranted = 0
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) changeToLeader() {
	if rf.role != Candidate{
		return
	}
	//defer Log().Debug.Printf("server %d, change to leader at term:%d",rf.me,rf.currentTerm)
	rf.role = Leader
	for i := range rf.nextIndex{
		rf.nextIndex[i] = len(rf.logs)
	}

	//match index should reset to 0
	for i:= range rf.matchIndex{
		rf.matchIndex[i] = 0
	}
}
