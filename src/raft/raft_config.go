package raft

import "time"

const(
	heartBeatTimeout = 150 * time.Millisecond
	electionTimeout = 300 * time.Millisecond

)