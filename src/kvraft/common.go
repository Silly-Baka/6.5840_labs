package kvraft

import (
	"log"
	"time"
)

const (
	Debug = true

	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "timeout"
	CkLock         = false
	KvLock         = true

	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"

	RETRY_TIMEOUT      = 800 * time.Millisecond
	Snapshot_CheckTime = 30 * time.Millisecond
	CheckPointFactor   = 0.4
)

type Err string

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	Err         Err
	CommitIndex int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}
