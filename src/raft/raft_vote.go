package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//


//
// example RequestVote RPC handler.
//

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//


type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.lock()
	defer rf.unlock()


	LogInstance().Debug.Printf("server %d recv voteRequest from candidate server: %d",rf.me,args.CandidateId)
	LogInstance().Info.Printf("args.term:%d, rf.term:%d",args.Term,rf.currentTerm)
	LogInstance().Info.Printf("vote for:%d, candidate:%d",rf.voteFor,args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//shorter logs or logs aren't up to date
	//restrict vote according to figure 5.4
	if rf.getLastLogIndex() < args.LastLogTerm || args.LastLogTerm < rf.logs[rf.getLastLogIndex()].Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term >= rf.currentTerm{
		rf.currentTerm = args.Term
		rf.changeRole(Follower)
	}
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId{
		rf.voteFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		LogInstance().Debug.Printf("server %d vote for server %d",rf.me,args.CandidateId)
	}else{
		LogInstance().Warning.Printf("server %d vote for args.CandidateId is %d",rf.me,args.CandidateId)
	}
	rf.resetElectionTimer()
	LogInstance().Debug.Printf("end vote rpc handler")
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	LogInstance().Info.Printf("server %d sends requestVote to server %d",rf.me,server)
	return ok
}

func (rf *Raft) startElection() {
	LogInstance().Info.Printf("server %d start election..",rf.me)
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.votesGranted = 1
	rf.resetElectionTimer()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	
	for i:= range rf.peers{
		if i == rf.me{
			continue
		}

		go func(peerIdx int, args *RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peerIdx,args,&reply)
			if ok{
				rf.lock()
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.changeRole(Follower)
				} else{
					if reply.VoteGranted && rf.role == Candidate {
						rf.votesGranted += 1
						if rf.votesGranted > len(rf.peers)/2{
							rf.changeRole(Leader)
						}
					}
				}
				rf.unlock()
			}else{
				LogInstance().Error.Printf("votes rpc from server %d to server %d not ok",rf.me,peerIdx)
			}
		}(i,&args)
	}

}