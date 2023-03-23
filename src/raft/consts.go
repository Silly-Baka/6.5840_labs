package raft

import "time"

const (
	Follower  = 0x1
	Candidate = 0x2
	Leader    = 0x3

	HeartBeatTimeout = time.Duration(120) * time.Millisecond
)
