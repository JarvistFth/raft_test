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

	//Log().Debug.Printf("server %d recv voteRequest from candidate server: %d args.term:%d, rf.term:%d vote for:%d, candidate:%d",
	//	rf.me,args.CandidateId,args.Term,rf.currentTerm,rf.voteFor,args.CandidateId)

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	//For example, if you have already voted in the current term,
	//and an incoming RequestVote RPC has a higher term that you,
	//you should first step down and adopt their term (thereby resetting votedFor),
	//and then handle the RPC, which will result in you granting the vote!
	if args.Term > rf.currentTerm{
		//Log().Info.Printf("s%d, reqvote args.Term > rf.currentTerm",rf.me)
		rf.changeToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false


	//Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm || ( rf.voteFor != -1 && rf.voteFor != args.CandidateId){
		//Log().Warning.Printf("reqvote args.Term < rf.currentTerm")
		return
	}


	//shorter logs or logs aren't up to date
	//restrict vote according to figure 5.4
	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.LastLogTerm < rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()){
		//Log().Warning.Printf("restrict log is up to date")
		return
	}

	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.role = Follower
	rf.persist()
	//you should only restart your election timer
	//if a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated,
	//you should not reset your timer);
	//b) you are starting an election; or
	//c) you grant a vote to another peer.
	rf.sendCh(rf.voteCh)
	//Log().Debug.Printf("server %d vote for server %d",rf.me,args.CandidateId)

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//Log().Info.Printf("server %d sends requestVote to server %d",rf.me,server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	//Log().Info.Printf("server %d start election..",rf.me)
	//defer rf.persist()

	rf.lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.unlock()

	
	for i:= range rf.peers{
		if i == rf.me{
			continue
		}

		go func(peerIdx int, args *RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(peerIdx,args,&reply)
			if ok{
				rf.lock()
				defer rf.unlock()

				//From experience, we have found that by far the simplest thing to do is to first record the term in the reply
				//(it may be higher than your current term), and then to compare the current term with the term you sent in your original RPC.
				//If the two are different, drop the reply and return.
				//Only if the two terms are the same should you continue processing the reply.
				if rf.currentTerm != args.Term || rf.role != Candidate{
					return
				}

				//If RPC request or response contains term T > currentTerm:
				//set currentTerm = T, convert to follower (§5.1)
				if reply.Term > rf.currentTerm{
					rf.changeToFollower(reply.Term)
					return
				}

				//If votes received from majority of servers: become leader
				if reply.VoteGranted {
					rf.votesGranted += 1
					if rf.votesGranted > len(rf.peers)/2{
						rf.changeToLeader()
						rf.sendCh(rf.voteCh)
					}
				}
			}
		}(i,&args)
	}
}