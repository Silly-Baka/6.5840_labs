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

// todo 尚未完善的Entry定义
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
	log         []LogEntry // log
	myNextIndex int        // the next index of the log
	commitIndex int        // the highest index of log has been committed
	lastApplied int        // the highest index of log has been replied to local state machine
	nextIndex   []int      // the next index of log will be sent to follower
	matchIndex  []int      // maintained with AppendEntries()
	applyCh     chan ApplyMsg
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		//DPrintf("[%v] failed to vote for [%v]", rf.me, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		DPrintf("[%v] find a new higher term and convert to follower", rf.me)
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && args.CandidateId != rf.votedFor {
		//DPrintf("[%v] failed to vote for [%v]", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		return
	}

	// restriction
	if rf.myNextIndex > 1 {
		lastLogTerm := rf.log[rf.myNextIndex-1].Term
		if lastLogTerm == args.LastLogTerm {
			if rf.myNextIndex-1 > args.LastLogIndex {
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}
		} else if lastLogTerm > args.LastLogTerm {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	}

	// grant vote and reset election timeout
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	rf.electionTimer.Reset(getElectionTimeout())

	//DPrintf("[%v] success to vote for [%v]", rf.me, args.CandidateId)
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
	DPrintf("[%v] cur log is %v", rf.me, rf.log)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		//DPrintf("[%v] reject the heartbeat from [%v]", rf.me, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		DPrintf("[%v] find a new higher term and convert to follower", rf.me)
	}
	rf.electionTimer.Reset(getElectionTimeout())

	//DPrintf("[%v] agree the heartbeat from [%v]", rf.me, args.LeaderId)

	// 2B
	// have found out the conflicting log
	if args.PrevLogIndex >= 0 && rf.myNextIndex > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

		// delete the conflicting log
		xTerm := rf.log[args.PrevLogIndex].Term
		xIndex := args.PrevLogIndex
		// can change to use binarySearch
		for xIndex >= 0 && rf.log[xIndex].Term == xTerm {
			xIndex--
		}
		xIndex++

		rf.myNextIndex = xIndex
		reply.XTerm = xTerm
		reply.XIndex = xIndex

		if xIndex > 0 {
			rf.log = rf.log[:xIndex]
		}

		return
	}
	//
	//// if log less than leader and no append entries（heartbeat check)
	//if args.PrevLogIndex+1 >= rf.myNextIndex && len(args.Entries) == 0 {
	//	reply.Success = false
	//	reply.XIndex = rf.myNextIndex
	//
	//	return
	//}

	// append entries if have new entries
	for _, entry := range args.Entries {
		// if the log longer than leader
		if rf.myNextIndex > args.PrevLogIndex+1 {
			// cut the conflicting log
			rf.log = rf.log[:args.PrevLogIndex]
			rf.myNextIndex = args.PrevLogIndex
		}
		rf.log = append(rf.log, entry)
		rf.myNextIndex++
	}

	// check commitIndex and reply the entry
	if args.LeaderCommit > rf.commitIndex {
		// commit entry and reset commitIndex
		if args.LeaderCommit < rf.myNextIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.myNextIndex - 1
		}

		// send applyMsg to kv server
		if rf.lastApplied < rf.commitIndex {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.commitIndex].Command,
				CommandIndex: rf.commitIndex,
			}
			rf.applyCh <- applyMsg

			rf.lastApplied = rf.commitIndex
		}
		DPrintf("[%v] follower commitIndex change to %v", rf.me, rf.commitIndex)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

	if rf.state != Leader || rf.killed() {
		rf.mu.Unlock()
		return 0, 0, false
	}
	// wrap the command into entry
	NewEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, NewEntry)
	index := rf.myNextIndex
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.myNextIndex++
	rf.mu.Unlock()

	DPrintf("[%v] leader start new command, index is %v", rf.me, index)

	// sync the log immediately
	go rf.leaderAppendEntries()

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
	//z := atomic.LoadInt32(&rf.dead)
	return false
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
			rf.newElection()
			rf.electionTimer.Reset(getElectionTimeout())
			//case <-testTimer.C:
			//	DPrintf("[%v] still waiting for election timeout", rf.me)
			//	testTimer.Reset(300 * time.Millisecond)
		}
	}
	DPrintf("error!!!!!!!")
}

// start a new election
func (rf *Raft) newElection() {
	// reset election timeout

	rf.mu.Lock()
	rf.currentTerm += 1

	// vote for self
	rf.votedFor = rf.me

	//if !rf.electionTimer.Stop() {
	//	<-rf.electionTimer.C
	//}
	rf.electionTimer.Reset(getElectionTimeout())

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[rf.myNextIndex-1].Term,
		LastLogIndex: rf.myNextIndex - 1,
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

			if rf.killed() {
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

			// find a new higher term and convert to follower
			if reply.Term > rf.currentTerm {
				DPrintf("[%v] find a new higher term and convert to follower", rf.me)

				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				//rf.electionTimer.Reset(getElectionTimeout())

				// todo
				voteCh <- false
				return
			}

			if reply.VoteGranted {
				//DPrintf("[%v] success get vote from [%v]", rf.me, server)
			} else {
				//DPrintf("[%v] failed to get vote from [%v]", rf.me, server)
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

			// init nextIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.myNextIndex
			}

			rf.mu.Unlock()

			DPrintf("[%v] become leader", rf.me)
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
	ms := 200 + (rand.Int63() % 100)
	//time := time.Duration(ms) * time.Millisecond
	//DPrintf("[%v] new election time:[%v]", rf.me, time)
	return time.Duration(ms) * time.Millisecond
}

// control the leader's heartbeat cycle
func (rf *Raft) heartbeat() {

	// todo : the bug which wasted my two days, no buffer channel
	//DoneCh := make(chan bool)
	//DoneCh := make(chan bool, 1)

	// initialized heartbeat
	go rf.doHeartbeat()
	//DPrintf("[%v] heartbeat [%v]", rf.me, ct)

	heartBeatTimer := time.NewTicker(HeartBeatTimeout)
	isDone := false

	cond := sync.Cond{L: &rf.mu}

	go func() {
		for {
			select {
			case <-heartBeatTimer.C:
				rf.mu.Lock()
				// stop heartbeat if not leader
				if rf.killed() || rf.state != Leader {
					isDone = true
					heartBeatTimer.Stop()

					rf.mu.Unlock()

					cond.Broadcast()
					return
				}
				rf.mu.Unlock()
				//DPrintf("[%v] heartbeat [%v]", rf.me, ct)
				go rf.doHeartbeat()

				DPrintf("[%v] leader, nextIndex array is %v", rf.me, rf.nextIndex)

				heartBeatTimer.Reset(HeartBeatTimeout)

			case <-rf.doneCh:
				rf.mu.Lock()
				isDone = true
				heartBeatTimer.Stop()
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
func (rf *Raft) doHeartbeat() {

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	prevLogIndex := 0
	prevLogTerm := 0
	if rf.myNextIndex > 1 {
		prevLogIndex = rf.myNextIndex - 1
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	// send appendEntries to each peer
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: leaderCommit,
			}

			reply := AppendEntriesReply{}

			if rf.killed() || rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			//DPrintf("[%v] leader send heartbeat to [%v]", rf.me, server)
			if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
				return
			}

			rf.mu.Lock()
			// find higher term, convert to follower
			if reply.Term > rf.currentTerm {
				DPrintf("[%v] find a new higher term and convert to follower", rf.me)

				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				//rf.electionTimer.Reset(getElectionTimeout())
				rf.mu.Unlock()

				rf.doneCh <- false

				return
			}

			// follower's log is later than leader
			//if !reply.Success && reply.XIndex != -1 {
			//	rf.nextIndex[server] = reply.XIndex
			//	rf.matchIndex[server] = reply.XIndex - 1
			//
			//	// start appendEntries
			//	//go func(sv int) {
			//	//	for {
			//	//		rf.mu.Lock()
			//	//		args := AppendEntriesArgs{
			//	//			Term:         currentTerm,
			//	//			LeaderId:     rf.me,
			//	//			PrevLogIndex: prevLogIndex,
			//	//			PrevLogTerm:  prevLogTerm,
			//	//			LeaderCommit: leaderCommit,
			//	//		}
			//	//		nextIndex := rf.myNextIndex
			//	//		tmpIndex := rf.nextIndex[sv]
			//	//
			//	//		// appendEntries() only if have new Entries
			//	//		if rf.nextIndex[sv] < nextIndex {
			//	//			args.Entries = rf.log[rf.nextIndex[sv]:nextIndex]
			//	//		}
			//	//		reply := AppendEntriesReply{}
			//	//
			//	//		if rf.killed() || rf.state != Leader {
			//	//			rf.mu.Unlock()
			//	//			return
			//	//		}
			//	//		rf.mu.Unlock()
			//	//
			//	//		DPrintf("[%v] leader send appendEntries to [%v] : %v", rf.me, sv, args)
			//	//		if ok := rf.sendAppendEntries(sv, &args, &reply); !ok && len(args.Entries) > 0 {
			//	//			// maybe the peer crash, we should retry if necessary（have new entries)
			//	//			continue
			//	//		}
			//	//
			//	//		rf.mu.Lock()
			//	//		// find higher term, convert to follower
			//	//		if reply.Term > rf.currentTerm {
			//	//			DPrintf("[%v] find a new higher term and convert to follower", rf.me)
			//	//
			//	//			rf.currentTerm = reply.Term
			//	//			rf.state = Follower
			//	//			rf.votedFor = -1
			//	//			//rf.electionTimer.Reset(getElectionTimeout())
			//	//			rf.mu.Unlock()
			//	//
			//	//			return
			//	//		}
			//	//		rf.mu.Unlock()
			//	//
			//	//		if reply.Success {
			//	//			rf.mu.Lock()
			//	//			// defend another later heartbeat change it
			//	//			if rf.nextIndex[server] == tmpIndex {
			//	//				rf.nextIndex[server] = nextIndex
			//	//				rf.matchIndex[server] = nextIndex - 1
			//	//			}
			//	//			rf.mu.Unlock()
			//	//			return
			//	//		} else {
			//	//			rf.mu.Lock()
			//	//			// decrement the nextIndex and retry AppendEntries()
			//	//			if reply.XTerm != -1 {
			//	//				rf.nextIndex[server] = reply.XIndex
			//	//				// todo
			//	//				rf.matchIndex[server] = reply.XIndex - 1
			//	//			}
			//	//			rf.mu.Unlock()
			//	//		}
			//	//	}
			//	//}(server)
			//}
			rf.mu.Unlock()
		}(idx)
	}
}

// the real logic of heartbeat: send AppendEntries() to each peer if become leader
func (rf *Raft) leaderAppendEntries() {

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	prevLogIndex := 0
	prevLogTerm := 0
	if rf.myNextIndex > 1 {
		prevLogIndex = rf.myNextIndex - 1
		prevLogTerm = rf.log[prevLogIndex].Term
	}
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
			for {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: leaderCommit,
				}
				nextIndex := rf.myNextIndex
				tmpIndex := rf.nextIndex[server]

				// appendEntries() only if have new Entries
				if rf.nextIndex[server] < nextIndex {
					args.Entries = rf.log[rf.nextIndex[server]:nextIndex]
				}
				reply := AppendEntriesReply{}

				if rf.killed() || rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				DPrintf("[%v] leader send appendEntries to [%v] : %v", rf.me, server, args)
				if ok := rf.sendAppendEntries(server, &args, &reply); !ok && len(args.Entries) > 0 {
					// maybe the peer crash, we should retry if necessary（have new entries)
					continue
				}

				rf.mu.Lock()
				// find higher term, convert to follower
				if reply.Term > rf.currentTerm {
					DPrintf("[%v] find a new higher term and convert to follower", rf.me)

					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					//rf.electionTimer.Reset(getElectionTimeout())
					rf.mu.Unlock()

					return
				}
				rf.mu.Unlock()

				if reply.Success {
					successCount.Add(1)
					if int(successCount.Load()) > len(rf.peers)/2 {
						isMajorityAgree.Store(true)
						cond.Broadcast()
					}
					rf.mu.Lock()
					// defend another later heartbeat change it
					if rf.nextIndex[server] == tmpIndex {
						rf.nextIndex[server] = nextIndex
						rf.matchIndex[server] = nextIndex - 1
					}
					rf.mu.Unlock()

					return
				} else {
					rf.mu.Lock()

					// decrement the nextIndex and retry AppendEntries()
					if reply.XTerm != -1 {
						rf.nextIndex[server] = reply.XIndex
						// todo
						rf.matchIndex[server] = reply.XIndex - 1
					}

					rf.mu.Unlock()
				}
			}
		}(idx)
	}
	rf.mu.Lock()
	// waiting for majority follower ack the appendEntries
	if !isMajorityAgree.Load() {
		cond.Wait()
	}
	DPrintf("[%v] leader success append entries to majority followers", rf.me)
	// update commitIndex
	for idx := rf.myNextIndex - 1; idx > rf.commitIndex; idx-- {
		count := 1
		if rf.log[idx].Term == rf.currentTerm {
			for _, v := range rf.matchIndex {
				if v >= idx {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = idx

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[idx].Command,
				CommandIndex: idx,
			}
			// send appMsg
			rf.lastApplied = idx
			rf.applyCh <- applyMsg
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

	rf.myNextIndex = 1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
