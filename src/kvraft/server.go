package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method   string   // get、put、append
	Args     []string // Args[0] = key , Args[1] = value
	ClientId int64
	Seq      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 3A
	DB           *DataBase
	doneChPool   map[string]chan interface{}
	DuplicateMap map[int64]int              // map that record each client's last Seq
	waitingChMap map[int]chan RequestResult // the map that record all the goroutines that waiting for command executed
	// 3B
	LastIncludedIndex int
	threshold         float64 // the threshold of RaftStateSize, snapshot when >= threshold
}

type DataBase struct {
	Data map[string]string
}

type RequestResult struct {
	value string
	err   Err
}

func (db *DataBase) Get(key string) (string, Err) {
	val, ok := db.Data[key]
	if !ok {
		val = ""
		return val, ErrNoKey
	}
	return val, OK
}

func (db *DataBase) Put(key string, value string) {

	db.Data[key] = value
}

func (db *DataBase) Append(key string, value string) {
	val, ok := db.Data[key]
	if !ok {
		val = ""
	}
	val += value

	db.Data[key] = val
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	tmpTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] server doing get [%v], client is [%v] seq is [%v]", kv.me, args.Key, args.ClientId, args.Seq)

	opr := Op{
		Method:   GET,
		Args:     []string{args.Key},
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}

	// check if the command is logged successfully
	commandIndex, term, _ := kv.rf.Start(opr)
	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}
	realNextIndex := kv.rf.RealNextIndex
	DPrintf("[%v] server doing get [%v], commandIndex is [%v], realNextIndex is [%v]", kv.me, args.Key, commandIndex, realNextIndex)

	waitingCh := kv.createWaitingCh(commandIndex)
	defer kv.deleteWaitingCh(commandIndex)

	timer := time.NewTimer(RETRY_TIMEOUT)

	select {

	case res := <-waitingCh:

		DPrintf("[%v] rpc applier get the result", kv.me)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		reply.Err = res.err
		reply.Value = res.value

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrTimeOut

		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	tmpTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opr := Op{
		Method:   args.Op,
		Args:     []string{args.Key, args.Value},
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	commandIndex, term, _ := kv.rf.Start(opr)

	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] server doing %v [%v : %v], seq is [%v], CommandIndex is [%v], realNextIndex is [%v]", kv.me, args.Op, args.Key, args.Value, args.Seq, commandIndex, kv.rf.RealNextIndex)

	waitingCh := kv.createWaitingCh(commandIndex)
	defer kv.deleteWaitingCh(commandIndex)

	timer := time.NewTimer(RETRY_TIMEOUT)

	select {

	case res := <-waitingCh:

		DPrintf("[%v] rpc applier get the result", kv.me)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		reply.Err = res.err

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrTimeOut

		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() Method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()

	// Your code here, if desired.
	kv.lock("Kill")
	defer kv.unlock("Kill")

	for _, ch := range kv.doneChPool {
		ch <- true
	}
	DPrintf("[%v] was killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(DataBase{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	// 3A
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.doneChPool = make(map[string]chan interface{})
	kv.DuplicateMap = make(map[int64]int)
	kv.waitingChMap = make(map[int]chan RequestResult)

	db := new(DataBase)
	db.Data = make(map[string]string)
	kv.DB = db

	// 3B
	kv.LastIncludedIndex = 0
	kv.threshold = CheckPointFactor * float64(kv.maxraftstate)

	// todo
	snapshot := kv.rf.Persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		kv.restoreBySnapshot(snapshot)
	}

	go kv.applier()

	return kv
}

// applier that listen to applyCh and handle the command
func (kv *KVServer) applier() {
	doneCh := kv.createDoneCh("applier")
	defer kv.deleteDoneCh("applier")

	timer := time.NewTimer(Snapshot_CheckTime)

	if kv.maxraftstate == -1 {
		timer.Stop()
	}

	for {
		select {
		case applyMsg := <-kv.applyCh:

			if applyMsg.CommandValid && applyMsg.Command != nil {
				DPrintf("[%v] applyIndex is [%v], realNextIndex is [%v]", kv.me, applyMsg.CommandIndex, kv.rf.RealNextIndex)

				// throw the Command that included in the snapshot
				if applyMsg.CommandIndex <= kv.LastIncludedIndex {
					break
				}

				opr, _ := applyMsg.Command.(Op)

				res := kv.apply(opr)

				DPrintf("[%v] success handler command [%v]", kv.me, applyMsg.CommandIndex)

				kv.LastIncludedIndex = applyMsg.CommandIndex

				kv.lock("applier")
				ch, ok := kv.waitingChMap[applyMsg.CommandIndex]
				if ok {
					if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
						kv.unlock("applier")
						break
					}
					DPrintf("[%v] sending res to chan [%v]", kv.me, applyMsg.CommandIndex)
					ch <- res
					DPrintf("[%v] success send result res to chan [%v]", kv.me, applyMsg.CommandIndex)
				}
				kv.unlock("applier")

			} else if applyMsg.SnapshotValid {

				DPrintf("[%v] snapshot index is [%v]", kv.me, applyMsg.SnapshotIndex)
				kv.restoreBySnapshot(applyMsg.Snapshot)
			}

		case <-timer.C:
			// check whether we should snapshot( >= 0.6 * maxsize
			if float64(kv.rf.Persister.RaftStateSize()) >= kv.threshold {
				kv.snapshot()
			}
			timer.Reset(Snapshot_CheckTime)

		case <-doneCh:

			DPrintf("[%v] server applier is dead", kv.me)

			if !timer.Stop() {
				<-timer.C
			}

			return
		}
	}
	DPrintf("[%v] server applier is dead", kv.me)
}

// the real logic of apply
func (kv *KVServer) apply(opr Op) RequestResult {
	// check if outdated
	maxSeq := kv.DuplicateMap[opr.ClientId]

	result := RequestResult{}

	switch opr.Method {
	case GET:
		v, err := kv.DB.Get(opr.Args[0])
		result.err = err
		result.value = v

	case PUT:
		if opr.Seq > maxSeq {
			kv.DB.Put(opr.Args[0], opr.Args[1])
			kv.DuplicateMap[opr.ClientId] = opr.Seq
		}
		result.err = OK
	case APPEND:
		if opr.Seq > maxSeq {
			kv.DB.Append(opr.Args[0], opr.Args[1])
			kv.DuplicateMap[opr.ClientId] = opr.Seq
		}
		result.err = OK
	}

	return result
}

// real logic of snapshot, encode state to []byte
func (kv *KVServer) snapshot() {

	DPrintf("[%v] server is snapshotting, lastInclude is [%v], realNext is [%v]", kv.me, kv.LastIncludedIndex, kv.rf.RealNextIndex)
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)

	if encoder.Encode(&kv.DB) != nil ||
		encoder.Encode(&kv.DuplicateMap) != nil ||
		encoder.Encode(&kv.LastIncludedIndex) != nil {
		panic(fmt.Sprintf("[%v] snapshot error", kv.me))
	}

	kv.rf.Snapshot(kv.LastIncludedIndex, buf.Bytes())
}

func (kv *KVServer) restoreBySnapshot(snapshot []byte) {

	decoder := gob.NewDecoder(bytes.NewBuffer(snapshot))

	var db DataBase
	var duplicateMap map[int64]int
	var lastIncludedIndex int

	if decoder.Decode(&db) != nil ||
		decoder.Decode(&duplicateMap) != nil ||
		decoder.Decode(&lastIncludedIndex) != nil {

		panic(fmt.Sprintf("[%v] restore by snapshot error", kv.me))
	}

	DPrintf("[%v] recovering from snapshot [%v]", kv.me, lastIncludedIndex)

	kv.DB = &db
	kv.DuplicateMap = duplicateMap
	kv.LastIncludedIndex = lastIncludedIndex
}

func (kv *KVServer) lock(name string) {
	kv.mu.Lock()
	if KvLock {
		DPrintf("[%v] function [%v()] get this lock", kv.me, name)
	}
}
func (kv *KVServer) unlock(name string) {
	kv.mu.Unlock()
	if KvLock {
		DPrintf("[%v] function [%v()] unlock", kv.me, name)
	}
}

func (kv *KVServer) createWaitingCh(commitIndex int) chan RequestResult {
	kv.lock("createWaitingCh")
	defer kv.unlock("createWaitingCh")

	ch := make(chan RequestResult)
	kv.waitingChMap[commitIndex] = ch

	return ch
}

func (kv *KVServer) deleteWaitingCh(commitIndex int) {
	kv.lock("deleteWaitingCh")
	defer kv.unlock("deleteWaitingCh")

	ch := kv.waitingChMap[commitIndex]

	if ch != nil {
		delete(kv.waitingChMap, commitIndex)
		close(ch)
	}
}

func (kv *KVServer) createDoneCh(name string) <-chan interface{} {
	//rf.mu.Lock()
	kv.lock("createDoneCh")
	defer kv.unlock("createDoneCh")

	DPrintf("[%v] channel %v has been created", kv.me, name)

	doneCh := make(chan interface{}, 1)
	kv.doneChPool[name] = doneCh

	return doneCh
}

func (kv *KVServer) deleteDoneCh(name string) {
	//rf.mu.Lock()
	kv.lock("deleteDoneCh")
	defer kv.unlock("deleteDoneCh")

	doneCh := kv.doneChPool[name]

	if doneCh != nil {
		delete(kv.doneChPool, name)
		close(doneCh)
	}

	DPrintf("[%v] channel %v has been closed", kv.me, name)
}
