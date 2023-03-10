package raft

// AppendEntries()
type AppendEntriesArgs struct {
	// information of this peer
	Term     int
	LeaderId int // index of the leader peer

	//
	Entries      []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}
