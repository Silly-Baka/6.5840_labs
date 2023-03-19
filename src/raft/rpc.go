package raft

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
}
