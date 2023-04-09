package raft

import "fmt"

// AppendEntries()
type AppendEntriesArgs struct {
	// information of this peer
	Term     int
	LeaderId int // index of the leader peer
	// 2B
	Entries      []LogEntry
	PrevLogIndex int // the index before the latest log
	PrevLogTerm  int // ...
	LeaderCommit int // leader's committed index
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // the term of conflicting log , -1 if no conflict
	XIndex  int // the first index of XTerm, -1 if no conflict
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntriesArgs{Term:%d, LeaderId:%d, Entries:%v, PrevLogIndex:%d, PrevLogTerm:%d, LeaderCommit:%d}",
		args.Term, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
}

// InstallSnapshot()
type InstallSnapshotArgs struct {
	Term              int // the term of follower
	LeaderId          int
	LastIncludedIndex int    // the last log index included in the snapshot
	LastIncludedTerm  int    // the last log term included in the snapshot
	Snapshot          []byte // the snapshot which stored in leader
}
type InstallSnapshotReply struct {
	Term int // the term of this peer
}
