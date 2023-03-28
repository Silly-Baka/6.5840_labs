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
	persister *Persister          // Object to hold this peer's persisted state
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
	doneCh        chan bool   // the channel that control the life of heartbeat

	// 2B
	log            []LogEntry // log
	realNextIndex  int        // the real next index of the log
	logicNextIndex int        // the logic next index of the log : start from baseIndex
	commitIndex    int        // the highest index of log has been committed
	lastApplied    int        // the highest index of log has been replied to local state machine
	nextIndex      []int      // the next index of log will be sent to follower
	matchIndex     []int      // maintained with AppendEntries()
	applyCh        chan ApplyMsg
	applierCh      chan bool

	// 2D
	lastIncludedTerm  int
	lastIncludedIndex int
	baseIndex         int // realIndex = index-baseIndex
	snapshot          []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
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

	raftState := buf.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
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

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil {

		panic("fail to read persist")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.realNextIndex = rf.logicToReal(len(log))
	rf.logicNextIndex = len(log)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index >= rf.realNextIndex {
		panic("the snapshot error")
	}
	// ignore the repeated snapshot
	if index == rf.lastIncludedIndex {
		return
	}
	logicIndex := rf.realToLogic(index)
	rf.lastIncludedTerm = rf.log[logicIndex].Term
	rf.lastIncludedIndex = index

	rf.log = append([]LogEntry{}, rf.log[logicIndex+1:]...)
	rf.logicNextIndex = len(rf.log)

	rf.baseIndex = index + 1
	DPrintf("[%v] snapshot last index is %v", rf.me, index)
	DPrintf("[%v] cur log is %v", rf.me, rf.log)
	rf.snapshot = snapshot

	if rf.state == Leader {
		// check nextIndex and send Snapshot to peer
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Snapshot:          snapshot,
		}

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.nextIndex[peer] <= index {
				go func(server int) {

					DPrintf("[%v] sending snapshot to [%v]", rf.me, server)
					reply := InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(server, &args, &reply); !ok {
						return
					}
					rf.mu.Lock()
					// throw the overdue reply
					if rf.currentTerm != args.Term || rf.state != Leader {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.electionTimer.Reset(getElectionTimeout())
					}
					rf.mu.Unlock()
				}(peer)
			}
			rf.nextIndex[peer] = rf.realNextIndex
		}
	}
}

// Leader calls this to update Followers' state
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.electionTimer.Reset(getElectionTimeout())
	}
	// replace the current snapshot
	if args.LastIncludedIndex > rf.lastIncludedIndex || args.LastIncludedIndex >= rf.realNextIndex {
		rf.snapshot = args.Snapshot

		// discard the log entries recovered by snapshot
		lastIncludeIndex := rf.realToLogic(args.LastIncludedIndex)
		if lastIncludeIndex < rf.logicNextIndex && rf.log[lastIncludeIndex].Term == args.LastIncludedTerm {

			rf.log = append([]LogEntry{}, rf.log[lastIncludeIndex+1:]...)
			rf.logicNextIndex = len(rf.log)

		} else {
			rf.log = []LogEntry{}
			rf.logicNextIndex = 0
		}
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.baseIndex = args.LastIncludedIndex + 1
		rf.realNextIndex = args.LastIncludedIndex + 1

		DPrintf("[%v] get snapshot from leader [%v], lastInclude is [%v]", rf.me, args.LeaderId, args.LastIncludedIndex)
		DPrintf("[%v] cur log is %v", rf.me, rf.log)
	}

	reply.Term = rf.currentTerm
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		if rf.realNextIndex-1 > args.LastLogIndex {
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

	//DPrintf("[%v] get heartbeat from [%v]", rf.me, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("[%v] term %v ,cur log is %v, entries is %v", rf.me, rf.currentTerm, rf.log, args)

	reply.XIndex = rf.baseIndex
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
	if args.PrevLogIndex >= rf.realNextIndex || preLogIndex >= 0 && rf.log[preLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

		// have no entry in prevLogIndex
		if args.PrevLogIndex >= rf.realNextIndex {
			reply.XTerm = -1
			reply.XIndex = rf.realNextIndex
		} else {
			// delete the conflicting log
			xTerm := rf.log[preLogIndex].Term
			xIndex := preLogIndex
			// can change to use binarySearch
			for xIndex >= 0 && rf.log[xIndex].Term == xTerm {
				xIndex--
			}
			rf.realNextIndex = rf.logicToReal(xIndex + 1)
			rf.logicNextIndex = xIndex + 1

			rf.log = rf.log[:xIndex+1]
			reply.XTerm = xTerm
			reply.XIndex = rf.logicToReal(xIndex + 1)

			rf.persist()
		}
		DPrintf("[%v] follower's xIndex is %v", rf.me, reply.XIndex)

		return
	}

	// append / modify  entries
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		logicIndex := rf.realToLogic(index)

		if index >= rf.realNextIndex || logicIndex >= 0 && rf.log[logicIndex].Term != entry.Term {
			rf.log = rf.log[:logicIndex]

			shard := args.Entries[i:]
			rf.log = append(rf.log, append([]LogEntry{}, shard...)...)
			rf.logicNextIndex = logicIndex + len(shard)
			rf.realNextIndex = index + len(shard)

			DPrintf("[%v] success append entries, cur log is [%v]", rf.me, rf.log)

			rf.persist()
			break
		}
	}
	rf.persist()

	// check commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// commit entry and reset commitIndex
		if args.LeaderCommit < rf.realNextIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.realNextIndex - 1
		}
		DPrintf("[%v] follower commitIndex change to %v", rf.me, rf.commitIndex)

		go func() {
			rf.applierCh <- true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() {
		return 0, 0, false
	}
	// wrap the command into entry
	NewEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, NewEntry)
	index := rf.realNextIndex
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.realNextIndex++
	rf.logicNextIndex++

	rf.persist()

	DPrintf("[%v] leader start new command, index is %v", rf.me, index)
	DPrintf("[%v] leader cur log is %v", rf.me, rf.log)
	// sync the log immediately
	go rf.doAppendEntries(false)

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	//testTimer := time.NewTimer(300 * time.Millisecond)
	for rf.killed() == false {
		// Your code here (2A)
		//Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock()
				break
			}
			rf.state = Candidate

			rf.mu.Unlock()

			DPrintf("[%v] election timeout, start new election", rf.me)
			rf.NewElection()
		}
	}
	DPrintf("[%v] leader is dead", rf.me)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		select {
		case <-rf.applierCh:
			rf.mu.Lock()
			if rf.lastApplied < rf.commitIndex {
				DPrintf("[%v] doing apply", rf.me)

				idx := rf.lastApplied + 1
				lastApplied := rf.realToLogic(rf.lastApplied)
				commitIndex := rf.realToLogic(rf.commitIndex)

				var applyEntries []LogEntry
				// have snapshot
				if rf.snapshot != nil && lastApplied <= 0 {
					applyMsg := ApplyMsg{
						SnapshotValid: true,
						Snapshot:      rf.snapshot,
						SnapshotTerm:  rf.lastIncludedTerm,
						SnapshotIndex: rf.lastIncludedIndex,
					}
					rf.applyCh <- applyMsg
					applyEntries = append([]LogEntry{}, rf.log[:commitIndex+1]...)
				} else {
					applyEntries = append([]LogEntry{}, rf.log[lastApplied+1:commitIndex+1]...)
				}
				rf.lastApplied = rf.commitIndex

				DPrintf("[%v] apply from [%v] to index [%v]", rf.me, idx, rf.lastApplied)
				DPrintf("[%v] cur log is %v", rf.me, rf.log)
				for i, entry := range applyEntries {
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: idx + i,
					}
					rf.applyCh <- applyMsg
					DPrintf("[%v] apply index is [%v]", rf.me, applyMsg.CommandIndex)
				}
				DPrintf("[%v] finish the apply", rf.me)
			}
			rf.mu.Unlock()
		}
	}
}

// start a new election
func (rf *Raft) NewElection() {
	// reset election timeout

	rf.mu.Lock()
	rf.currentTerm += 1
	DPrintf("[%v] cur term is %v", rf.me, rf.currentTerm)

	// vote for self
	rf.votedFor = rf.me

	rf.persist()

	rf.electionTimer.Reset(getElectionTimeout())

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.realNextIndex - 1,
	}
	if rf.logicNextIndex == 0 {
		args.LastLogTerm = rf.lastIncludedTerm
	} else {
		args.LastLogTerm = rf.log[rf.logicNextIndex-1].Term
	}
	rf.mu.Unlock()

	total := len(rf.peers)
	voteCh := make(chan bool, total)
	// get vote from each peer
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		// request vote async
		go func(server int) {

			if rf.killed() || rf.state != Candidate {
				return
			}
			reply := RequestVoteReply{}
			//DPrintf("[%v] request vote to [%v]", rf.me, server)
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				//DPrintf("[%v] failed to request the peer [%v]", rf.me, server)
				voteCh <- false
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// term has been changed（become follower), or has been Leader
			if rf.currentTerm != args.Term || rf.state != Candidate || rf.killed() {

				voteCh <- false
				return
			}

			//if reply.VoteGranted {
			//	DPrintf("[%v] success get vote from %v", rf.me, server)
			//} else {
			//	DPrintf("[%v] failed to get vote from %v", rf.me, server)
			//}

			// find a new higher term and convert to follower
			if reply.Term > rf.currentTerm {
				DPrintf("[%v] find a new higher term and convert to follower", rf.me)

				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.electionTimer.Reset(getElectionTimeout())

				voteCh <- false
				rf.persist()
				return
			}
			voteCh <- reply.VoteGranted
		}(idx)
	}

	voteCount := 1
	oprCount := 1
	// wait for result / how to do if timeout
	for voteGranted := range voteCh {

		rf.mu.Lock()

		// higher term and become follower, or timeout（term has been incr)
		if rf.state != Candidate || rf.currentTerm != args.Term || rf.killed() {
			DPrintf("[%v] failed in the election", rf.me)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if voteGranted {
			voteCount++
		}
		// get majority vote and become leader
		if voteCount > total/2 {
			rf.mu.Lock()
			rf.state = Leader

			// init nextIndex and matchIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.realNextIndex
				rf.matchIndex[i] = 0
			}

			DPrintf("[%v] become leader, cur log is %v", rf.me, rf.log)
			rf.mu.Unlock()

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

// get election timeout between 200ms ~ 400ms randomly
func getElectionTimeout() time.Duration {
	ms := 200 + (rand.Int63() % 200)
	//time := time.Duration(ms) * time.Millisecond
	//DPrintf("[%v] new election time:[%v]", rf.me, time)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) realToLogic(realIndex int) int {
	return realIndex - rf.baseIndex
}
func (rf *Raft) logicToReal(logicIndex int) int {
	return logicIndex + rf.baseIndex
}

// control the leader's heartbeat cycle
func (rf *Raft) heartbeat() {

	// initialized heartbeat
	go rf.doAppendEntries(true)
	//DPrintf("[%v] heartbeat [%v]", rf.me, ct)

	heartBeatTimer := time.NewTimer(HeartBeatTimeout)
	isDone := false

	cond := sync.Cond{L: &rf.mu}

	go func() {
		for !rf.killed() {
			select {
			case <-heartBeatTimer.C:
				rf.mu.Lock()
				// stop heartbeat if not leader
				if rf.killed() || rf.state != Leader {
					isDone = true
					go func() {
						if !heartBeatTimer.Stop() {
							<-heartBeatTimer.C
						}
					}()

					rf.mu.Unlock()

					DPrintf("[%v] stop the heartBeat", rf.me)

					cond.Broadcast()
					return
				}
				rf.mu.Unlock()
				//DPrintf("[%v] heartbeat [%v]", rf.me, ct)
				go rf.doAppendEntries(true)

				heartBeatTimer.Reset(HeartBeatTimeout)

			case <-rf.doneCh:
				rf.mu.Lock()
				isDone = true
				if !heartBeatTimer.Stop() {
					<-heartBeatTimer.C
				}
				rf.mu.Unlock()

				cond.Broadcast()

				return
			}
		}
	}()
	rf.mu.Lock()
	for !isDone {
		cond.Wait()
	}
	rf.mu.Unlock()
}

// the real logic of heartbeat: send AppendEntries() to each peer if become leader
func (rf *Raft) doAppendEntries(isHeartBeat bool) {

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	cond := sync.Cond{L: &rf.mu}
	successCount := atomic.Int32{}
	successCount.Store(1)
	isMajorityAgree := atomic.Bool{}

	// send appendEntries to each peer
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			for !rf.killed() && rf.state == Leader {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: leaderCommit,
				}

				// todo
				preLogIndex := rf.realToLogic(rf.nextIndex[server] - 1)
				if preLogIndex < 0 {
					args.PrevLogIndex = rf.lastIncludedIndex
					args.PrevLogTerm = rf.lastIncludedTerm
				} else {
					args.PrevLogIndex = rf.nextIndex[server] - 1
					args.PrevLogTerm = rf.log[preLogIndex].Term
				}

				// appendEntries() if have new Entries
				if rf.realToLogic(args.PrevLogIndex+1) < rf.logicNextIndex {
					args.Entries = rf.log[rf.realToLogic(args.PrevLogIndex+1):]
				}
				reply := AppendEntriesReply{}

				baseIndex := rf.baseIndex

				if rf.killed() || rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
					// maybe the peer crash, we should retry
					return
				}

				rf.mu.Lock()

				// throw the overdue reply
				if rf.currentTerm != args.Term || rf.state != Leader || rf.baseIndex != baseIndex {
					rf.mu.Unlock()
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
					rf.mu.Unlock()

					return
				}
				rf.mu.Unlock()

				DPrintf("[%v] peer %v reply is %v", rf.me, server, reply)
				if reply.Success {
					successCount.Add(1)
					if int(successCount.Load()) > len(rf.peers)/2 {
						isMajorityAgree.Store(true)
						if !isHeartBeat {
							cond.Broadcast()
						}
					}
					rf.mu.Lock()
					// defend another later heartbeat change it
					nextIndex := args.PrevLogIndex + len(args.Entries) + 1
					rf.nextIndex[server] = nextIndex
					rf.matchIndex[server] = nextIndex - 1

					DPrintf("[%v] %v's nextIndex is %v", rf.me, server, rf.nextIndex[server])

					rf.mu.Unlock()

					return
				} else {
					rf.mu.Lock()

					DPrintf("[%v] follower %v refuse, xTerm is %v, xIndex is %v", rf.me, server, reply.XTerm, reply.XIndex)
					rf.nextIndex[server] = reply.XIndex

					DPrintf("[%v] %v's nextIndex is %v", rf.me, server, rf.nextIndex[server])
					rf.mu.Unlock()
				}
			}
		}(idx)
	}
	// we should wait for majority agree for the new entries if AppendEntries()
	if !isHeartBeat {
		DPrintf("[%v] leader success append entries to majority followers", rf.me)

		rf.mu.Lock()
		// waiting for majority follower ack the appendEntries
		if !isMajorityAgree.Load() {
			cond.Wait()
		}
		rf.mu.Unlock()
	}

	rf.mu.Lock()

	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	// check matchIndex and update commitIndex only we remain the leader
	for idx := rf.realNextIndex - 1; idx > rf.commitIndex; idx-- {
		count := 1
		logicIndex := rf.realToLogic(idx)
		if rf.log[logicIndex].Term == rf.currentTerm {
			for _, v := range rf.matchIndex {
				if v >= idx {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = idx
			go func() {
				rf.applierCh <- true
			}()
			DPrintf("[%v] leader commitIndex is %v", rf.me, idx)

			break
		}
	}
	rf.mu.Unlock()
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
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(getElectionTimeout())
	rf.currentTerm = 0
	rf.doneCh = make(chan bool, 1)

	// 2B
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.realNextIndex = 1
	rf.logicNextIndex = 1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applierCh = make(chan bool, 1000)

	// 2C
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 2D
	rf.baseIndex = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// start ticker goroutine to start elections
	go rf.ticker() // start applier goroutine to listen apply entries
	go rf.applier()

	return rf
}
