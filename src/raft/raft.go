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

	//	"6.5840/labgob"
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
	Term int    // the term when receive the command
	Data []byte // command
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
	currentTerm   int        // latest term this raft has seen
	votedFor      int        // index of target peer
	log           []LogEntry // log todo: 待完善 当前用不到
	state         int        // follower、candidate、leader
	electionTimer time.Timer // timer that record election timeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

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

	// reset election timeout
	rf.electionTimer.Reset(rf.getElectionTimeout())

	rf.mu.Lock()
	term := rf.currentTerm

	if args.Term < term {
		rf.mu.Unlock()
		reply.Term = term
		reply.VoteGranted = false
		DPrintf("[%v] failed to vote for [%v]", rf.me, args.CandidateId)
		return
	}
	if args.Term > term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%v] failed to vote for [%v]", rf.me, args.CandidateId)
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.mu.Unlock()

	reply.VoteGranted = true
	DPrintf("[%v] success to vote for [%v]", rf.me, args.CandidateId)

	//var lastLogTerm int
	//var lastLogIndex int
	//if len(rf.log) > 0 {
	//	lastLogTerm = rf.log[len(rf.log)-1].Term
	//	lastLogIndex = len(rf.log) - 1
	//}
	//if args.LastLogTerm > lastLogTerm {
	//
	//}
	//

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

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	//l := len(rf.log)
	rf.mu.Unlock()

	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}
	if args.Term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.mu.Unlock()
	}

	reply.Success = true
	rf.electionTimer.Reset(rf.getElectionTimeout())

	return
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		select {
		// blocking and wait for election timeout
		case <-rf.electionTimer.C:
			DPrintf("[%v] election timeout, start new election", rf.me)
			rf.mu.Lock()

			switch rf.state {
			case Leader:
				rf.mu.Unlock()
				break

			case Follower:
				rf.mu.Unlock()
				// become candidate and start a new election
				rf.state = Candidate
				go rf.newElection()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// start a new election
func (rf *Raft) newElection() {
	rf.mu.Lock()
	rf.currentTerm++

	// vote for self
	rf.votedFor = rf.me

	// reset election timeout
	rf.electionTimer.Reset(rf.getElectionTimeout())

	lastLogTerm := 0
	lastLogIndex := 0
	l := len(rf.log)
	if l > 0 {
		lastLogTerm = rf.log[l-1].Term
		lastLogIndex = l
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}
	rf.mu.Unlock()

	voteCh := make(chan bool, l)
	// get vote from each peer
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		// request vote async
		go func(server int) {
			reply := RequestVoteReply{}
			DPrintf("[%v] request vote to [%v]", rf.me, server)
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				DPrintf("[%v] failed to request the peer [%v]", rf.me, server)
				return
			}
			rf.mu.Lock()
			currentTerm := rf.currentTerm
			rf.mu.Unlock()

			// find a new higher term and convert to follower
			if reply.Term > currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.mu.Unlock()

				voteCh <- false
				return
			}
			if reply.VoteGranted {
				DPrintf("[%v] success get vote from [%v]", rf.me, server)
			} else {
				DPrintf("[%v] failed to get vote from [%v]", rf.me, server)
			}
			voteCh <- reply.VoteGranted
		}(idx)
	}

	voteCount := 1
	oprCount := 0
	total := len(rf.peers)
	// wait for result / how to do if timeout
	for voteGranted := range voteCh {

		// record only candidate
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		if voteGranted {
			voteCount++
			// get majority vote and become leader
			if voteCount > total/2 {
				rf.mu.Lock()
				rf.state = Leader
				rf.votedFor = -1
				rf.mu.Unlock()

				go rf.heartbeat()

				break
			}
		}
		oprCount++
		// stop the blocking if get all result
		if oprCount == total {
			break
		}
	}
}

// get election timeout between 200ms ~ 400ms randomly
func (rf *Raft) getElectionTimeout() time.Duration {
	ms := 200 + (rand.Int63() % 201)

	return time.Duration(ms) * time.Millisecond
}

// control the leader's heartbeat cycle
func (rf *Raft) heartbeat() {

	DoneCh := make(chan bool)
	// initialized heartbeat
	rf.doHeartbeat(&DoneCh)

	heartBeatTimer := time.NewTimer(HeartBeatTimeout)
	isDone := false

	cond := sync.Cond{L: &rf.mu}

	go func() {
		select {
		case <-heartBeatTimer.C:
			rf.mu.Lock()
			// stop heartbeat if not leader
			if rf.state != Leader {
				rf.mu.Unlock()
				heartBeatTimer.Stop()
				isDone = true

				cond.Broadcast()
				break
			}
			rf.mu.Unlock()

			rf.doHeartbeat(&DoneCh)
			heartBeatTimer.Reset(HeartBeatTimeout)

		case <-DoneCh:
			// stop heartbeat
			heartBeatTimer.Stop()
			cond.Broadcast()

			break
		}
	}()

	rf.mu.Lock()
	for !isDone {
		cond.Wait()
	}
}

// the real logic of heartbeat: send AppendEntries() to each peer if become leader
func (rf *Raft) doHeartbeat(ch *chan bool) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	prevLogIndex := 0
	prevLogTerm := 0
	l := len(rf.log)
	if l > 0 {
		prevLogIndex = l - 1
		prevLogTerm = rf.log[l-1].Term
	}
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	// send appendEntries to each peer
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			DPrintf("[%v] leader send heartbeat to [%v]", rf.me, server)
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				DPrintf("[%v] failed to send heartbeat to [%v]", rf.me, server)
				return
			}
			// find higher term, convert to follower
			if reply.Term > currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.mu.Unlock()

				*ch <- false
			}
		}(idx)
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
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.electionTimer = *time.NewTimer(rf.getElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
