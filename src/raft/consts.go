package raft

import "time"

const (
	Follower  = 0x1
	Candidate = 0x2
	Leader    = 0x3

	NONE_lOG = -1

	HeartBeatTimeout = time.Duration(100) * time.Millisecond
)
