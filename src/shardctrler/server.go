package shardctrler

import (
	"6.5840/raft"
	"math/rand"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.
	configs  []Config    // indexed by config num
	shardMap map[int]int // shardId --> GID

	replicaGroups  map[int][]string           // GID --> names of replica in this group
	duplicateTable map[int64]int              // clientId --> maxSeq
	waitingChPool  map[int]chan RequestResult //
	applierDoneCh  chan interface{}
}

type Op struct {
	// Your data here.
	ClientId int64
	Seq      int
	Method   string        // join、leave、move、query
	Args     []interface{} // the args of command
}

type RequestResult struct {
	config Config
	err    Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	tmpTerm, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	opr := Op{
		Method:   Join,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Args:     []interface{}{args.Servers},
	}
	commandIndex, term, _ := sc.rf.Start(opr)

	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	waitingCh := sc.createWaitingCh(commandIndex)

	timer := time.NewTimer(getRetryTimeout())

	select {

	case res := <-waitingCh:

		sc.deleteWaitingCh(commandIndex)

		DPrintf("[%v] rpc applier get the result", sc.me)
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		}

		reply.Err = res.err
		reply.WrongLeader = false

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrTimeOut
		timer.Stop()

		go func() {
			defer sc.deleteWaitingCh(commandIndex)

			<-waitingCh
		}()

		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	tmpTerm, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	opr := Op{
		Method:   Leave,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Args:     []interface{}{args.GIDs},
	}
	commandIndex, term, _ := sc.rf.Start(opr)

	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	waitingCh := sc.createWaitingCh(commandIndex)

	timer := time.NewTimer(getRetryTimeout())

	select {

	case res := <-waitingCh:

		sc.deleteWaitingCh(commandIndex)

		DPrintf("[%v] rpc applier get the result", sc.me)
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		}

		reply.Err = res.err
		reply.WrongLeader = false

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrTimeOut
		timer.Stop()

		go func() {
			defer sc.deleteWaitingCh(commandIndex)

			<-waitingCh
		}()

		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	tmpTerm, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	opr := Op{
		Method:   Move,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Args:     []interface{}{args.Shard, args.GID},
	}
	commandIndex, term, _ := sc.rf.Start(opr)

	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	waitingCh := sc.createWaitingCh(commandIndex)

	timer := time.NewTimer(getRetryTimeout())

	select {

	case res := <-waitingCh:

		sc.deleteWaitingCh(commandIndex)

		DPrintf("[%v] rpc applier get the result", sc.me)
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		}

		reply.Err = res.err
		reply.WrongLeader = false

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
		timer.Stop()

		go func() {
			defer sc.deleteWaitingCh(commandIndex)

			<-waitingCh
		}()

		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	tmpTerm, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	opr := Op{
		Method:   Leave,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Args:     []interface{}{args.Num},
	}
	commandIndex, term, _ := sc.rf.Start(opr)

	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	waitingCh := sc.createWaitingCh(commandIndex)

	timer := time.NewTimer(getRetryTimeout())

	select {

	case res := <-waitingCh:

		sc.deleteWaitingCh(commandIndex)

		DPrintf("[%v] rpc applier get the result", sc.me)
		_, isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return
		}

		reply.Err = res.err
		reply.WrongLeader = false
		reply.Config = res.config

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrTimeOut
		reply.WrongLeader = false

		timer.Stop()

		go func() {
			defer sc.deleteWaitingCh(commandIndex)

			<-waitingCh
		}()

		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	DPrintf("[%v] shardctler was killed", sc.me)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}

func (sc *ShardCtrler) applier() {
	doneCh := make(chan interface{})
	sc.applierDoneCh = doneCh

	defer func() {
		sc.applierDoneCh = nil
		close(doneCh)
	}()

	//timer := time.NewTimer(Snapshot_CheckTime)

	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {

				opr, _ := applyMsg.Command.(Op)

				res := sc.apply(opr)

				DPrintf("[%v] success handler command [%v]", sc.me, applyMsg.CommandIndex)

				sc.lock("applier")
				ch, ok := sc.waitingChPool[applyMsg.CommandIndex]
				sc.unlock("applier")

				if ok {
					if term, isLeader := sc.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
						break
					}
					//DPrintf("[%v] sending res to chan [%v]", kv.me, applyMsg.CommandIndex)
					ch <- res
					//DPrintf("[%v] success send result res to chan [%v]", kv.me, applyMsg.CommandIndex)
				}

			}
		case <-doneCh:

			DPrintf("[%v] server applier is dead", sc.me)

			return
		}
	}
}

func (sc *ShardCtrler) apply(opr Op) RequestResult {
	// check if outdated
	maxSeq := sc.duplicateTable[opr.ClientId]

	result := RequestResult{}

	switch opr.Method {

	case Join:

		joinServers, _ := opr.Args[0].(map[int][]string)
		// check whether repeated request
		if opr.Seq > maxSeq {
			// append into replicaGroups
			for gid, servers := range joinServers {
				group, ok := sc.replicaGroups[gid]
				if !ok {
					group = make([]string, 0)
				}
				group = append(group, servers...)

				sc.replicaGroups[gid] = group
			}
			sc.rehash()
			// todo: make new Configuration

		}

	case Leave:

		GIDS, _ := opr.Args[0].([]int)
		if opr.Seq > maxSeq {

			// remove replica Groups in the GIDS
			for _, GID := range GIDS {
				delete(sc.replicaGroups, GID)
			}
			// todo: move the shard within dropped Groups
			sc.rehash()
		}

	case Move:

		shard, _ := opr.Args[0].(int)
		GID, _ := opr.Args[1].(int)

		if opr.Seq > maxSeq {
			// move the shard to GID
			sc.shardMap[shard] = GID

			// todo: make new configuration

		}
	case Query:

		num, _ := opr.Args[0].(int)
		if opr.Seq > maxSeq {
			maxNum := len(sc.configs) - 1

			if num == -1 || num > maxNum {
				// todo: make new configuration and return

			} else {
				result.config = sc.configs[num]
			}
		}
	}
	result.err = OK

	return result
}

// rehash the shards with Consistent Hash
func (sc *ShardCtrler) rehash() {

}

func (sc *ShardCtrler) lock(name string) {
	sc.mu.Lock()

	DPrintf("[%v] function [%v()] get this lock", sc.me, name)
}
func (sc *ShardCtrler) unlock(name string) {
	sc.mu.Unlock()

	DPrintf("[%v] function [%v()] unlock", sc.me, name)
}

func (sc *ShardCtrler) createWaitingCh(commitIndex int) chan RequestResult {
	sc.lock("createWaitingCh")
	defer sc.unlock("createWaitingCh")

	ch := make(chan RequestResult)
	sc.waitingChPool[commitIndex] = ch

	return ch
}

func (sc *ShardCtrler) deleteWaitingCh(commitIndex int) {
	sc.lock("deleteWaitingCh")
	defer sc.unlock("deleteWaitingCh")

	ch := sc.waitingChPool[commitIndex]

	if ch != nil {
		delete(sc.waitingChPool, commitIndex)
		close(ch)
	}
}

//
//func (sc *ShardCtrler) createDoneCh(name string) <-chan interface{} {
//	//rf.mu.Lock()
//	sc.lock("createDoneCh")
//	defer sc.unlock("createDoneCh")
//
//	DPrintf("[%v] channel %v has been created", sc.me, name)
//
//	doneCh := make(chan interface{}, 1)
//	sc.doneChPool[name] = doneCh
//
//	return doneCh
//}
//
//func (kv *KVServer) deleteDoneCh(name string) {
//	//rf.mu.Lock()
//	kv.lock("deleteDoneCh")
//	defer kv.unlock("deleteDoneCh")
//
//	doneCh := kv.doneChPool[name]
//
//	if doneCh != nil {
//		delete(kv.doneChPool, name)
//		close(doneCh)
//	}
//
//	DPrintf("[%v] channel %v has been closed", kv.me, name)
//}}

func getRetryTimeout() time.Duration {

	ms := 500 + (rand.Int63() % 400)

	return time.Duration(ms) * time.Millisecond
}
