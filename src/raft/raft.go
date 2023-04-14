package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	CommandTerm int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int         // the term when receive the command
	Command interface{} // command
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currentTerm   int         // latest term this raft has seen
	votedFor      int         // index of target peer
	state         int         // follower、candidate、leader
	electionTimer *time.Timer // timer that record election timeout

	// 2B
	log            []LogEntry // log
	RealNextIndex  int        // the real next index of the log
	logicNextIndex int        // the logic next index of the log : start from baseIndex
	commitIndex    int        // the highest index of log has been committed
	lastApplied    int        // the highest index of log has been replied to local state machine
	nextIndex      []int      // the next index of log will be sent to follower
	matchIndex     []int      // maintained with AppendEntries()
	applyCh        chan ApplyMsg
	applierCh      chan interface{} // a buffer that store the applyMsg

	// 2D
	lastIncludedTerm  int
	lastIncludedIndex int // logicIndex = index-lastIncludedIndex
	snapshot          []byte

	// rebuild
	replicatorChPool []chan AppendEntriesArgs
	doneChPool       map[string]chan interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock("GetState")
	defer rf.unlock("GetState")

	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)

	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)

	encoder.Encode(rf.RealNextIndex)
	// we need more information about snapshot
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.lastIncludedTerm)

	raftState := buf.Bytes()
	rf.Persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	decoder := labgob.NewDecoder(bytes.NewReader(data))
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var realNextIndex int
	var lastIncludedIndex int
	var lastIncludedTerm int

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil ||
		decoder.Decode(&realNextIndex) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&lastIncludedTerm) != nil {

		panic("fail to read persist")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor

	rf.log = log
	rf.RealNextIndex = realNextIndex
	rf.logicNextIndex = len(log)
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	DPrintf("[%v] peer was recovery", rf.me)
	// detail that we should recovery the lastApplied to the end of snapshot
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	rf.snapshot = rf.Persister.ReadSnapshot()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//rf.mu.Lock()
	rf.lock("Snapshot")
	//defer rf.mu.Unlock()
	defer rf.unlock("Snapshot")

	if index >= rf.RealNextIndex {
		panic(fmt.Sprintf("[%v] the snapshot error, lastInclude is [%v], realNext is [%v]", rf.me, index, rf.RealNextIndex))
	}
	logicIndex := rf.realToLogic(index)

	if logicIndex < 0 {
		return
	}

	lastIncludedTerm := rf.log[logicIndex].Term

	// ignore the repeated/delay snapshot
	if lastIncludedTerm <= rf.lastIncludedTerm && index <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm

	rf.log = append([]LogEntry{{rf.lastIncludedTerm, nil}}, rf.log[logicIndex+1:]...)
	rf.logicNextIndex = len(rf.log)
	rf.snapshot = snapshot

	if index >= rf.commitIndex {
		rf.lastApplied = index
		rf.commitIndex = index
	}

	rf.persist()
	DPrintf("[%v] snapshot last index is %v", rf.me, index)
	DPrintf("[%v] cur log is %v", rf.me, rf.log)
}

// Leader calls this to update Followers' state
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//rf.mu.Lock()
	rf.lock("InstallSnapshot")
	defer rf.unlock("InstallSnapshot")

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm

		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower

		rf.persist()
	}
	if rf.state != Follower {
		rf.state = Follower
	}
	rf.electionTimer.Reset(getElectionTimeout())

	// refuse repeated snapshot
	if args.LastIncludedTerm <= rf.lastIncludedTerm && args.LastIncludedIndex <= rf.lastIncludedIndex {
		//rf.unlock("InstallSnapshot")

		return
	}
	// refuse snapshot that
	DPrintf("[%v] apply index from [%v] to [%v]", rf.me, rf.lastApplied, args.LastIncludedIndex)

	// discard the log entries recovered by snapshot
	lastIncludeIndex := rf.realToLogic(args.LastIncludedIndex)

	//discardEntry := []LogEntry{}
	if lastIncludeIndex < rf.logicNextIndex && rf.log[lastIncludeIndex].Term == args.LastIncludedTerm {

		//discardEntry = append(discardEntry, rf.log[1:lastIncludeIndex+1]...)
		rf.log = append([]LogEntry{{args.LastIncludedTerm, nil}}, rf.log[lastIncludeIndex+1:]...)
		rf.logicNextIndex = len(rf.log)

	} else {
		//discardEntry = append(discardEntry, rf.log[1:]...)
		rf.log = []LogEntry{{args.LastIncludedTerm, nil}}
		rf.logicNextIndex = 1
		rf.RealNextIndex = args.LastIncludedIndex + 1
	}
	rf.snapshot = args.Snapshot
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// todo
	if rf.lastIncludedIndex >= rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persist()

	//rf.mu.Unlock()
	//rf.unlock("InstallSnapshot")

	DPrintf("[%v] get snapshot from leader [%v], lastInclude is [%v]", rf.me, args.LeaderId, args.LastIncludedIndex)
	DPrintf("[%v] cur log is %v", rf.me, rf.log)

	// apply snapshot to local state machine
	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		rf.applyCh <- applyMsg
	}()
	DPrintf("[%v] apply snapshot index is [%v] - [%v]", rf.me, 1, applyMsg.SnapshotIndex)
}

func (rf *Raft) doInstallSnapshot(server int, args InstallSnapshotArgs) {

	rf.lock("doInstallSnapshot")
	if rf.killed() || rf.state != Leader {
		//rf.mu.Unlock()
		rf.unlock("doInstallSnapshot")
		return
	}
	rf.unlock("doInstallSnapshot")

	DPrintf("[%v] sending snapshot to [%v]", rf.me, server)
	reply := InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(server, &args, &reply); !ok {
		return
	}
	DPrintf("[%v] success doInstallSnapshot to [%v]", rf.me, server)

	//rf.mu.Lock()
	rf.lock("doInstallSnapshot")
	// throw the overdue reply
	if rf.killed() || rf.currentTerm != args.Term || rf.state != Leader {
		//rf.mu.Unlock()
		rf.unlock("doInstallSnapshot")

		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.electionTimer.Reset(getElectionTimeout())

		rf.persist()
	}

	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex

	rf.unlock("doInstallSnapshot")
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // = peer's term if > currentTerm
	VoteGranted bool // true if got vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//DPrintf("[%v] get vote request from %v", rf.me, args.CandidateId)

	//rf.mu.Lock()
	rf.lock("RequestVote")
	//defer rf.mu.Unlock()
	defer rf.unlock("RequestVote")

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.electionTimer.Reset(getElectionTimeout())
		DPrintf("[%v] find a new higher term and convert to follower", rf.me)

		rf.persist()
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && args.CandidateId != rf.votedFor {
		//DPrintf("[%v] failed to vote for [%v]", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// vote restriction
	lastLogTerm := rf.log[rf.logicNextIndex-1].Term
	if lastLogTerm == args.LastLogTerm {
		if rf.RealNextIndex-1 > args.LastLogIndex {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	} else if lastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// grant vote and reset election timeout
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	// store the state
	rf.persist()

	rf.electionTimer.Reset(getElectionTimeout())
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//rf.mu.Lock()
	rf.lock("AppendEntries")
	//defer rf.mu.Unlock()
	defer rf.unlock("AppendEntries")

	DPrintf("[%v] get heartbeat from [%v]", rf.me, args.LeaderId)

	//DPrintf("[%v] term %v ,cur log is %v, entries is %v", rf.me, rf.currentTerm, rf.log, args)

	reply.XIndex = rf.lastIncludedIndex
	reply.XTerm = 0

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		DPrintf("[%v] find a new higher term and convert to follower", rf.me)

		rf.persist()
	}
	if rf.state != Follower {
		rf.state = Follower
	}
	rf.electionTimer.Reset(getElectionTimeout())

	// 2B
	// have no entry in prevLogIndex or find conflicting log
	preLogIndex := rf.realToLogic(args.PrevLogIndex)

	DPrintf("[%v] realNextIndex is [%v]\n logicNextIndex is [%v] \n lastIncludedIndex is [%v]\n preLogIndex is [%v]", rf.me, rf.RealNextIndex, rf.logicNextIndex, rf.lastIncludedIndex, preLogIndex)
	if args.PrevLogIndex >= rf.RealNextIndex || preLogIndex >= 0 && preLogIndex < rf.logicNextIndex && rf.log[preLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		DPrintf("[%v] heartbeat refuse and check", rf.me)
		// have no entry in prevLogIndex
		if args.PrevLogIndex >= rf.RealNextIndex {
			reply.XTerm = -1
			reply.XIndex = rf.RealNextIndex
		} else {
			// delete the conflicting log
			xTerm := rf.log[preLogIndex].Term
			xIndex := preLogIndex
			// can change to use binarySearch
			for xIndex > 0 && rf.log[xIndex].Term == xTerm {
				xIndex--
			}
			rf.RealNextIndex = rf.logicToReal(xIndex + 1)
			rf.logicNextIndex = xIndex + 1

			rf.log = rf.log[:xIndex+1]
			reply.XTerm = xTerm
			reply.XIndex = rf.logicToReal(xIndex + 1)

			//DPrintf("[%v] heartbeat refuse and persist", rf.me)
			rf.persist()
		}
		//DPrintf("[%v] heartbeat send refuse", rf.me)
		DPrintf("[%v] follower's xIndex is %v", rf.me, reply.XIndex)

		return
	}

	//DPrintf("[%v] heartbeat appendEntries", rf.me)
	// append / modify  entries
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		logicIndex := rf.realToLogic(index)

		if index >= rf.RealNextIndex || logicIndex > 0 && rf.log[logicIndex].Term != entry.Term {
			rf.log = rf.log[:logicIndex]

			shard := args.Entries[i:]
			// todo
			rf.log = append(rf.log, shard...)
			rf.logicNextIndex = logicIndex + len(shard)
			rf.RealNextIndex = index + len(shard)

			DPrintf("[%v] success append entries, cur log is [%v]", rf.me, rf.log)

			rf.persist()
			break
		}
	}

	// check commitIndex

	if args.LeaderCommit > rf.commitIndex {
		// commit entry and reset commitIndex
		if args.LeaderCommit < rf.RealNextIndex-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.RealNextIndex - 1
		}
		DPrintf("[%v] follower commitIndex change to %v", rf.me, rf.commitIndex)
		DPrintf("[%v] realNextIndex is [%v], lastIncludedIndex is [%v]", rf.me, rf.RealNextIndex, rf.lastIncludedIndex)

		// ask the applier
		go func() {
			rf.applierCh <- struct{}{}
		}()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrintf("[%v] leader, nextIndex array is %v", rf.me, rf.nextIndex)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	//rf.mu.Lock()
	rf.lock("Start")
	defer rf.unlock("Start")

	if rf.killed() || rf.state != Leader {
		return 0, 0, false
	}

	// wrap the command into entry
	NewEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, NewEntry)
	index := rf.RealNextIndex
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.RealNextIndex += 1
	rf.logicNextIndex += 1

	rf.persist()

	DPrintf("[%v] leader start new command, index is %v", rf.me, index)
	DPrintf("[%v] leader cur log is %v", rf.me, rf.log)
	// sync the log immediately
	go rf.doDispatchRPC(false)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.lock("Kill")
	defer rf.unlock("Kill")
	for _, doneCh := range rf.doneChPool {
		doneCh <- true
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	doneCh := rf.createDoneCh("ticker")
	defer rf.deleteDoneCh("ticker")

	//testTimer := time.NewTimer(300 * time.Millisecond)
	for {
		// Your code here (2A)
		//Check if a leader election should be started.
		startTime := time.Now()
		select {
		case tm := <-rf.electionTimer.C:
			//rf.mu.Lock()
			rf.lock("ticker")
			if rf.state == Leader {
				//rf.mu.Unlock()
				rf.unlock("ticker")
				break
			}
			rf.state = Candidate

			//rf.mu.Unlock()
			rf.unlock("ticker")

			difTime := time.Duration(tm.UnixMilli()-startTime.UnixMilli()) * time.Millisecond
			DPrintf("[%v] election timeout, start new election [%v]", rf.me, difTime)
			rf.NewElection()

		case <-doneCh:
			go func() {
				if !rf.electionTimer.Stop() {
					<-rf.electionTimer.C
				}
			}()
			DPrintf("[%v] peer is killed", rf.me)
			return
		}
	}
}

// goroutine that responsible for applyMsg
func (rf *Raft) applier() {
	doneCh := rf.createDoneCh("applier")
	defer rf.deleteDoneCh("applier")

	for {
		select {
		case <-rf.applierCh:
			//rf.mu.Lock()
			rf.lock("applier")

			if rf.lastApplied >= rf.commitIndex {
				//rf.mu.Unlock()
				rf.unlock("applier")
				break
			}

			lastApplied := rf.lastApplied
			commitIndex := rf.commitIndex
			logicLastApplied := rf.realToLogic(lastApplied)
			logicCommitIndex := rf.realToLogic(commitIndex)
			currentTerm := rf.currentTerm

			// have been snapshot, do not need to apply
			if logicLastApplied < 0 {
				//rf.mu.Unlock()
				rf.unlock("applier")
				break
			}

			entries := append([]LogEntry{}, rf.log[logicLastApplied+1:logicCommitIndex+1]...)

			DPrintf("[%v] apply from [%v] to index [%v]", rf.me, lastApplied, commitIndex)
			//rf.mu.Unlock()
			rf.unlock("applier")

			for i, entry := range entries {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: lastApplied + i + 1,
					CommandTerm:  currentTerm,
				}
				rf.applyCh <- applyMsg
				DPrintf("[%v] apply index is [%v]", rf.me, applyMsg.CommandIndex)
			}

			rf.lock("applier")
			if commitIndex > rf.lastApplied {
				rf.lastApplied = commitIndex
			}
			rf.unlock("applier")

		case <-doneCh:
			return
		}
	}
}

// start a new election
func (rf *Raft) NewElection() {
	// reset election timeout

	//rf.mu.Lock()
	rf.lock("NewElection")
	rf.currentTerm += 1
	DPrintf("[%v] cur term is %v", rf.me, rf.currentTerm)

	// vote for self
	rf.votedFor = rf.me

	rf.persist()

	rf.electionTimer.Reset(getElectionTimeout())

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.RealNextIndex - 1,
	}
	args.LastLogTerm = rf.log[rf.logicNextIndex-1].Term
	rf.unlock("NewElection")

	total := len(rf.peers)
	voteCh := make(chan bool, total)

	//defer func() {
	//	close(voteCh)
	//}()
	// get vote from each peer
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		// request vote async
		go func(server int) {

			rf.lock("NewElection")
			if rf.killed() || rf.state != Candidate {
				rf.unlock("NewElection")

				voteCh <- false
				return
			}
			rf.unlock("NewElection")

			reply := RequestVoteReply{}
			//DPrintf("[%v] request vote to [%v]", rf.me, server)
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				//DPrintf("[%v] failed to request the peer [%v]", rf.me, server)
				voteCh <- false
				return
			}
			rf.lock("NewElection")
			defer rf.unlock("NewElection")

			// term has been changed（become follower), or has been Leader
			if rf.currentTerm != args.Term || rf.state != Candidate || rf.killed() {

				voteCh <- false
				return
			}
			// find a new higher term and convert to follower
			if reply.Term > rf.currentTerm {
				DPrintf("[%v] find a new higher term and convert to follower", rf.me)

				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.electionTimer.Reset(getElectionTimeout())

				rf.persist()
				voteCh <- false
				return
			}
			voteCh <- reply.VoteGranted
		}(idx)
	}

	voteCount := 1
	oprCount := 1
	// wait for result / how to do if timeout
	for voteGranted := range voteCh {

		//rf.mu.Lock()
		rf.lock("NewElection")

		// higher term and become follower, or timeout（term has been incr)
		if rf.state != Candidate || rf.currentTerm != args.Term || rf.killed() {
			DPrintf("[%v] failed in the election", rf.me)
			//rf.mu.Unlock()
			rf.unlock("NewElection")

			return
		}
		//rf.mu.Unlock()
		rf.unlock("NewElection")

		if voteGranted {
			voteCount++
		}
		// get majority vote and become leader
		if voteCount > total/2 {
			//rf.mu.Lock()
			rf.lock("NewElection")
			rf.state = Leader

			// init nextIndex and matchIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.RealNextIndex
				rf.matchIndex[i] = 0
			}

			DPrintf("[%v] become leader, cur log is %v", rf.me, rf.log)
			//rf.mu.Unlock()
			rf.unlock("NewElection")

			go rf.heartbeat()

			break
		}
		oprCount++
		// stop the blocking if get all result
		if oprCount == total {
			break
		}
	}
}

// get election timeout between 200ms ~ 300ms randomly
func getElectionTimeout() time.Duration {
	ms := 200 + (rand.Int63() % 150)
	//time := time.Duration(ms) * time.Millisecond
	//DPrintf("[%v] new election time:[%v]", rf.me, time)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) realToLogic(realIndex int) int {
	return realIndex - rf.lastIncludedIndex
}
func (rf *Raft) logicToReal(logicIndex int) int {
	return logicIndex + rf.lastIncludedIndex
}

// control the leader's heartbeat cycle
func (rf *Raft) heartbeat() {
	// initialized heartbeat
	go rf.doDispatchRPC(true)
	//DPrintf("[%v] heartbeat [%v]", rf.me, ct)

	go func() {
		doneCh := rf.createDoneCh("heartbeatCh")
		defer func() {
			rf.deleteDoneCh("heartbeatCh")
			DPrintf("[%v] heartbeat stop", rf.me)
		}()

		heartBeatTimer := time.NewTimer(HeartBeatTimeout)

		for {
			select {
			case <-heartBeatTimer.C:
				rf.lock("heartbeat")
				// stop heartbeat if not leader
				if rf.killed() || rf.state != Leader {
					go func() {
						if !heartBeatTimer.Stop() {
							<-heartBeatTimer.C
						}
					}()
					rf.unlock("heartbeat")

					DPrintf("[%v] stop the heartBeat", rf.me)

					return
				}
				rf.unlock("heartbeat")

				go rf.doDispatchRPC(true)
				heartBeatTimer.Reset(HeartBeatTimeout)

			case <-doneCh:
				go func() {
					if !heartBeatTimer.Stop() {
						<-heartBeatTimer.C
					}
				}()
				DPrintf("[%v] peer was killed, heartbeat stop", rf.me)
				return
			}
		}
	}()
}

// dispatch the rpc
func (rf *Raft) doDispatchRPC(isHeartBeat bool) {

	//rf.mu.Lock()
	rf.lock("dispatchRPC")
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	//rf.mu.Unlock()
	rf.unlock("dispatchRPC")

	count := atomic.Int32{}
	count.Store(0)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.lock("doDispatchRPC")
		nextIndex := rf.nextIndex[i]
		lastIncludeIndex := rf.lastIncludedIndex
		lastIncludeTerm := rf.lastIncludedTerm
		snapshot := rf.snapshot
		realNextIndex := rf.RealNextIndex
		rf.unlock("doDispatchRPC")

		// if heartbeat, should send RPC immediately
		if isHeartBeat {

			if nextIndex < lastIncludeIndex {
				// send InstallSnapshot() RPC
				installSnapshotArgs := InstallSnapshotArgs{
					Term:              args.Term,
					LeaderId:          args.LeaderId,
					LastIncludedIndex: lastIncludeIndex,
					LastIncludedTerm:  lastIncludeTerm,
					Snapshot:          snapshot,
				}
				go rf.doInstallSnapshot(i, installSnapshotArgs)

			} else {
				go rf.doAppendEntries(i, args)
			}
		}

		if nextIndex < realNextIndex {
			go func(peer int) {
				rf.replicatorChPool[peer] <- args
			}(i)
		}
	}
}

// the real logic of heartbeat: send AppendEntries() to each peer if become leader
func (rf *Raft) doAppendEntries(server int, args AppendEntriesArgs) {

	//rf.mu.Lock()
	rf.lock("doAppendEntries")
	if rf.killed() || rf.state != Leader {
		//rf.mu.Unlock()
		rf.unlock("doAppendEntries")
		return
	}

	preLogIndex := rf.realToLogic(rf.nextIndex[server] - 1)

	// do not send entries before snapshot
	if preLogIndex < 0 {
		rf.unlock("doAppendEntries")
		return
	}

	if preLogIndex >= 0 {
		args.PrevLogTerm = rf.log[preLogIndex].Term
	}
	args.PrevLogIndex = rf.nextIndex[server] - 1

	if preLogIndex+1 > 0 && preLogIndex+1 < rf.logicNextIndex {
		args.Entries = rf.log[preLogIndex+1:]
	}
	//rf.mu.Unlock()
	rf.unlock("doAppendEntries")

	DPrintf("[%v] send heartbeat to [%v], args: [%v]", rf.me, server, args.String())
	reply := AppendEntriesReply{}

	//DPrintf("[%v] realNextIndex is [%v], lastIncludedIndex is [%v]", rf.me, rf.realNextIndex, rf.lastIncludedIndex)
	DPrintf("[%v] appendEntries to [%v] is [%v]", rf.me, server, args)

	if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
		// maybe the peer crash
		DPrintf("[%v] peer [%v] crash and have no reply", rf.me, server)
		return
	}

	//rf.mu.Lock()
	rf.lock("doAppendEntries")
	// throw the overdue reply
	if rf.killed() || rf.currentTerm != args.Term || rf.state != Leader {
		//rf.mu.Unlock()
		rf.unlock("doAppendEntries")
		return
	}

	// find higher term, convert to follower
	if reply.Term > rf.currentTerm {
		//DPrintf("[%v] find a new higher term and convert to follower", rf.me)

		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.electionTimer.Reset(getElectionTimeout())

		rf.persist()
		//rf.mu.Unlock()
		rf.unlock("doAppendEntries")

		return
	}
	//rf.mu.Unlock()
	rf.unlock("doAppendEntries")

	DPrintf("[%v] peer %v reply is %v", rf.me, server, reply)
	if reply.Success {
		//rf.mu.Lock()
		rf.lock("doAppendEntries")
		// defend another later heartbeat change it
		nextIndex := args.PrevLogIndex + len(args.Entries) + 1
		rf.nextIndex[server] = nextIndex
		rf.matchIndex[server] = nextIndex - 1

		DPrintf("[%v] %v's matchIndex is %v", rf.me, server, rf.matchIndex[server])

		// check matchIndex and update commitIndex only we remain the leader
		for idx := rf.RealNextIndex - 1; idx > rf.commitIndex; idx-- {
			logicIndex := rf.realToLogic(idx)

			// means that entries have been snapshot, do not need to apply
			if logicIndex <= 0 {
				break
			}
			if rf.log[logicIndex].Term == rf.currentTerm {
				count := 1
				for i, v := range rf.matchIndex {
					if i == rf.me {
						continue
					}
					if v >= idx {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = idx

					//rf.mu.Unlock()
					rf.unlock("doAppendEntries")

					// update and check apply
					rf.applierCh <- struct{}{}

					DPrintf("[%v] leader commitIndex change to  %v", rf.me, idx)

					return
				}
			}
		}
		//rf.mu.Unlock()
		rf.unlock("doAppendEntries")

		DPrintf("[%v] test dead lock [%v]", rf.me, server)
	} else {
		//rf.mu.Lock()
		rf.lock("doAppendEntries")
		DPrintf("[%v] follower %v refuse, xTerm is %v, xIndex is %v", rf.me, server, reply.XTerm, reply.XIndex)

		find := false
		idx := 0
		// find the index that beyond the last entry of XTerm
		for i, entry := range rf.log {
			if entry.Term == reply.XTerm {
				find = true
				idx = i
			}
		}
		if find {
			rf.nextIndex[server] = rf.logicToReal(idx + 1)
		} else {
			rf.nextIndex[server] = reply.XIndex
		}
		DPrintf("[%v] %v's nextIndex is %v", rf.me, server, rf.nextIndex[server])
		//rf.mu.Unlock()
		rf.unlock("doAppendEntries")
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(getElectionTimeout())
	rf.currentTerm = 0

	rf.replicatorChPool = make([]chan AppendEntriesArgs, len(peers))
	rf.doneChPool = make(map[string]chan interface{})

	// 2B
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.RealNextIndex = 1
	rf.logicNextIndex = 1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// 2D
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applierCh = make(chan interface{})

	// 2C
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.InitReplicator()
	go rf.ticker() // start applier goroutine to listen apply entries
	go rf.applier()

	return rf
}
func (rf *Raft) InitReplicator() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			name := fmt.Sprintf("replicator%d", server)
			doneCh := rf.createDoneCh(name)
			defer rf.deleteDoneCh(name)

			rf.lock("InitReplicator")
			rf.replicatorChPool[server] = make(chan AppendEntriesArgs, 1000)
			rf.unlock("InitReplicator")

			for {
				select {
				// do heartbeat or AppendEntries()
				case args := <-rf.replicatorChPool[server]:

					innerDoneCh := make(chan bool)
					go func() {
						for {
							select {
							case <-rf.replicatorChPool[server]:
							case <-innerDoneCh:
								return
							}
						}
					}()

					DPrintf("[%v] doing appendEntries to [%v]", rf.me, server)

					//rf.mu.Lock()
					rf.lock("InitReplicator")

					nextIndex := rf.nextIndex[server]
					lastIncludedIndex := rf.lastIncludedIndex
					lastIncludedTerm := rf.lastIncludedTerm
					snapshot := rf.snapshot

					rf.unlock("InitReplicator")

					// distinguish whether the log has been compacted
					if nextIndex > lastIncludedIndex {

						//DPrintf("[%v] follower [%v] delay in [%v], send AppendEntries", rf.me, server, rf.nextIndex[server])
						rf.doAppendEntries(server, args)

					} else {
						// send InstallSnapshot() RPC
						installSnapshotArgs := InstallSnapshotArgs{
							Term:              args.Term,
							LeaderId:          args.LeaderId,
							LastIncludedIndex: lastIncludedIndex,
							LastIncludedTerm:  lastIncludedTerm,
							Snapshot:          snapshot,
						}

						rf.doInstallSnapshot(server, installSnapshotArgs)
					}
					innerDoneCh <- true

				// close the goroutine
				case <-doneCh:
					return
				}
			}
		}(peer)
	}
}
func (rf *Raft) createDoneCh(name string) <-chan interface{} {
	//rf.mu.Lock()
	rf.lock("createDoneCh")
	defer rf.unlock("createDoneCh")

	doneCh := make(chan interface{}, 1)
	rf.doneChPool[name] = doneCh

	return doneCh
}

func (rf *Raft) deleteDoneCh(name string) {
	//rf.mu.Lock()
	rf.lock("deleteDoneCh")
	defer rf.unlock("deleteDoneCh")

	doneCh := rf.doneChPool[name]

	if doneCh != nil {
		close(doneCh)
		delete(rf.doneChPool, name)
	}

	DPrintf("[%v] channel %v has been closed", rf.me, name)
}

func (rf *Raft) lock(name string) {
	rf.mu.Lock()
	if Lock {
		DPrintf("[%v] function [%v()] get this lock", rf.me, name)
	}
}
func (rf *Raft) unlock(name string) {
	rf.mu.Unlock()
	if Lock {
		DPrintf("[%v] function [%v()] unlock", rf.me, name)
	}
}

func (rf *Raft) GetLastApplied() int {
	rf.lock("GetLastApplied")
	defer rf.unlock("GetLastApplied")

	return rf.lastApplied
}

func (rf *Raft) GetCommitIndex() int {
	rf.lock("GetCommitIndex")
	defer rf.unlock("GetCommitIndex")

	return rf.commitIndex
}

func (rf *Raft) GetRealNextIndex() int {
	rf.lock("GetRealNextIndex")
	defer rf.unlock("GetRealNextIndex")

	return rf.RealNextIndex
}
