package raft

import "time"

const(
	heartBeatTimeout = 100 * time.Millisecond
	electionTimeout = 300 * time.Millisecond

)