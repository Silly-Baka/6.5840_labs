package shardctrler

import (
	"6.5840/raft"
	"math/rand"
	"sort"
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
	configs []Config // indexed by config num

	shardMap        []int                      // shardId --> GID
	replicaGroups   map[int][]string           // GID --> names of replica in this group
	duplicateTable  map[int64]int              // clientId --> maxSeq
	waitingChPool   map[int]chan RequestResult //
	applierDoneCh   chan interface{}
	hashRing        *HashRing     // the hashRing that use for Consistent hash
	shardEachGroup  map[int][]int // GID --> []shard
	remainingShards []int
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
		reply.WrongLeader = true
		return
	}

	DPrintf("[%v] doing join [%v]", sc.me, args.Servers)

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
		reply.WrongLeader = true
		return
	}

	DPrintf("[%v] doing leave [%v]", sc.me, args.GIDs)

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
		reply.WrongLeader = true
		return
	}

	DPrintf("[%v] doing move [%v --> %v]", sc.me, args.Shard, args.GID)

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
		Method:   Query,
		ClientId: args.ClientId,
		Seq:      args.Seq,
		Args:     []interface{}{args.Num},
	}
	commandIndex, term, _ := sc.rf.Start(opr)

	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	DPrintf("[%v] doing query [%v]", sc.me, args.Num)

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

	sc.applierDoneCh <- struct{}{}

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
	labgob.Register(map[int][]string{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.shardMap = make([]int, NShards)
	for i := range sc.shardMap {
		sc.shardMap[i] = 0
	}
	sc.replicaGroups = make(map[int][]string)
	sc.replicaGroups[0] = []string{}

	sc.duplicateTable = make(map[int64]int)
	sc.waitingChPool = make(map[int]chan RequestResult)
	sc.hashRing = NewHashRing(VirtualNodeNum)
	sc.shardEachGroup = make(map[int][]int)
	sc.remainingShards = []int{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	}

	go sc.applier()

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

				DPrintf("[%v] success handle command [%v], method is [%v : %v]", sc.me, applyMsg.CommandIndex, opr.Method, opr.Args)

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
				sc.shardEachGroup[gid] = make([]int, 0)

				//// add replica group into HashRing
				//sc.hashRing.addNode(gid)

			}
			sc.rehash()

			// todo: make new Configuration
			sc.createNewConfig()

			sc.duplicateTable[opr.ClientId] = opr.Seq
		}

	case Leave:

		GIDS, _ := opr.Args[0].([]int)
		if opr.Seq > maxSeq {

			// remove replica Groups in the GIDS
			for _, GID := range GIDS {
				delete(sc.replicaGroups, GID)

				shards, ok := sc.shardEachGroup[GID]
				if ok {
					DPrintf("[%v] gid [%v] shard [%v] will be deleted", sc.me, GID, shards)
					for _, shard := range shards {
						sc.shardMap[shard] = 0
					}
					sc.remainingShards = append(sc.remainingShards, shards...)
					delete(sc.shardEachGroup, GID)
				}
			}
			sc.rehash()

			sc.createNewConfig()
			sc.duplicateTable[opr.ClientId] = opr.Seq
		}

	case Move:

		shard, _ := opr.Args[0].(int)
		GID, _ := opr.Args[1].(int)

		if opr.Seq > maxSeq {
			// remove the shard from origin Group
			originGid := sc.shardMap[shard]
			sds, ok := sc.shardEachGroup[originGid]
			if ok {
				l := len(sds)
				for i, v := range sds {
					if v == shard {
						// move the num after i to front
						for j := i; j < l-1; j++ {
							sds[j] = sds[j+1]
						}
						break
					}
				}
				sds = append([]int{}, sds[:l-1]...)
				sc.shardEachGroup[originGid] = sds
			}
			// move the shard to GID
			sc.shardMap[shard] = GID
			sc.shardEachGroup[GID] = append(sc.shardEachGroup[GID], shard)

			// todo: make new configuration
			sc.createNewConfig()

			sc.duplicateTable[opr.ClientId] = opr.Seq
		}
	case Query:

		num, _ := opr.Args[0].(int)
		//sc.lock("apply")
		maxNum := len(sc.configs) - 1

		if num == -1 || num > maxNum {
			result.config = sc.configs[maxNum]
		} else {
			// todo: maybe no concurrent problem ? just append
			// todo: if GC, need to lock
			result.config = sc.configs[num]
		}
	}
	result.err = OK

	return result
}

// rehash the shards with Consistent Hash
// use simpler algorithm because Consistent Hash could not pass the test
func (sc *ShardCtrler) rehash() {
	//for i := 0; i < NShards; i++ {
	//	gid := sc.hashRing.getNode(i)
	//	sc.shardMap[i] = gid
	//}
	//DPrintf("cur shardmap is [%v]", sc.shardMap)

	if len(sc.shardEachGroup) == 0 {
		return
	}

	DPrintf("[%v] rehashing, cur shards is [%v]", sc.me, sc.shardMap)
	DPrintf("[%v] rehasing, remainShards is [%v]", sc.me, sc.remainingShards)

	if len(sc.remainingShards) > 0 {
		for _, shard := range sc.remainingShards {
			minGid := sc.findGidWithMinShards()

			minShards := sc.shardEachGroup[minGid]
			minShards = append(minShards, shard)

			sc.shardMap[shard] = minGid
			sc.shardEachGroup[minGid] = minShards
		}
		sc.remainingShards = []int{}
	}

	// move the biggest to the smallest until dif < 1
	for {
		maxGid := sc.findGidWithMaxShards()
		minGid := sc.findGidWithMinShards()

		maxShards, _ := sc.shardEachGroup[maxGid]
		minShards, _ := sc.shardEachGroup[minGid]

		// dif < 1 means that strike balance
		dif := len(maxShards) - len(minShards)

		if dif <= 1 {
			break
		}
		for dif > 1 {
			minShards = append(minShards, maxShards[0])

			sc.shardMap[maxShards[0]] = minGid
			maxShards = maxShards[1:]
			dif -= 2
		}

		sc.shardEachGroup[minGid] = minShards
		sc.shardEachGroup[maxGid] = maxShards
	}
	DPrintf("[%v] finished rehash", sc.me)
}

func (sc *ShardCtrler) findGidWithMaxShards() int {
	GIDS := make([]int, 0)
	for gid := range sc.shardEachGroup {
		GIDS = append(GIDS, gid)
	}
	sort.Ints(GIDS)

	maxGid := -1
	maxLen := 0

	for _, gid := range GIDS {
		shards, ok := sc.shardEachGroup[gid]
		if !ok {
			continue
		}
		if len(shards) >= maxLen {
			maxLen = len(shards)
			maxGid = gid
		}
	}
	return maxGid
}

func (sc *ShardCtrler) findGidWithMinShards() int {
	GIDS := make([]int, 0)
	for gid := range sc.shardEachGroup {
		GIDS = append(GIDS, gid)
	}
	sort.Ints(GIDS)

	minGid := -1
	minLen := 0x7fffffff

	for _, gid := range GIDS {
		shards, ok := sc.shardEachGroup[gid]
		if !ok {
			continue
		}
		if len(shards) <= minLen {
			minLen = len(shards)
			minGid = gid
		}
	}
	return minGid
}

func (sc *ShardCtrler) createNewConfig() Config {
	//sc.lock("createNewConfig")
	//defer sc.unlock("createNewConfig")

	shards := [10]int{}
	for i, v := range sc.shardMap {
		shards[i] = v
	}
	groups := make(map[int][]string)
	for k, v := range sc.replicaGroups {
		if k == 0 {
			continue
		}
		groups[k] = append([]string{}, v...)
	}

	config := Config{
		Num:    len(sc.configs),
		Shards: shards,
		Groups: groups,
	}

	sc.configs = append(sc.configs, config)

	DPrintf("[%v] new shards distribution is [%v]", sc.me, shards)

	return config
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

	ms := 500 + (rand.Int63() % 500)

	return time.Duration(ms) * time.Millisecond
}
