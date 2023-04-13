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
	commitIndex, term, _ := kv.rf.Start(opr)
	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] server doing get [%v], commitIndex is [%v]", kv.me, args.Key, kv.rf.GetCommitIndex())

	waitingCh := kv.createWaitingCh(commitIndex)
	//defer kv.deleteWaitingCh(commitIndex)

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
	commitIndex, term, _ := kv.rf.Start(opr)

	DPrintf("[%v] server doing %v [%v : %v], seq is [%v]", kv.me, args.Op, args.Key, args.Value, args.Seq)
	if term != tmpTerm {
		reply.Err = ErrWrongLeader
		return
	}
	waitingCh := kv.createWaitingCh(commitIndex)
	//defer kv.deleteWaitingCh(commitIndex)

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
	kv.LastIncludedIndex = 0

	// todo
	snapshot := kv.rf.Persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.restoreBySnapshot(snapshot)
	}

	go kv.applier()

	if maxraftstate != -1 {
		go kv.snapshoter()
	}

	return kv
}

// applier that listen to applyCh and handle the command
func (kv *KVServer) applier() {
	doneCh := kv.createDoneCh("applier")
	defer kv.deleteDoneCh("applier")

	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:

			if applyMsg.CommandValid {
				opr, _ := applyMsg.Command.(Op)

				kv.lock("applier1")
				lastIncludedIndex := kv.LastIncludedIndex
				kv.unlock("applier1")

				DPrintf("[%v] commandIndex is [%v], lastIncludedIndex is [%v]", kv.me, applyMsg.CommandIndex, lastIncludedIndex)

				// throw the repeated entries that included in the snapshot
				if applyMsg.CommandIndex < lastIncludedIndex {
					break
				}

				res := kv.apply(opr)

				kv.lock("applier2")
				ch, ok := kv.waitingChMap[applyMsg.CommandIndex]

				kv.LastIncludedIndex = applyMsg.CommandIndex

				kv.unlock("applier2")

				if ok {
					if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
						break
					}
					ch <- res

					kv.deleteWaitingCh(applyMsg.CommandIndex)
				}
			} else if applyMsg.SnapshotValid && len(applyMsg.Snapshot) > 0 {
				kv.restoreBySnapshot(applyMsg.Snapshot)
			}
		case <-doneCh:

			DPrintf("[%v] server applier is dead", kv.me)
			return
		}
	}
	DPrintf("[%v] server applier is dead", kv.me)
}

// the real logic of apply
func (kv *KVServer) apply(opr Op) RequestResult {
	// check if outdated
	kv.lock("apply")
	maxSeq := kv.DuplicateMap[opr.ClientId]
	kv.unlock("apply")

	result := RequestResult{}

	switch opr.Method {
	case GET:
		kv.lock("apply")
		v, err := kv.DB.Get(opr.Args[0])
		kv.unlock("apply")

		result.err = err
		result.value = v
	case PUT:

		if opr.Seq > maxSeq {
			kv.lock("apply")
			kv.DB.Put(opr.Args[0], opr.Args[1])

			kv.DuplicateMap[opr.ClientId] = opr.Seq
			kv.unlock("apply")
		}
		result.err = OK
	case APPEND:
		if opr.Seq > maxSeq {
			kv.lock("apply")
			kv.DB.Append(opr.Args[0], opr.Args[1])

			kv.DuplicateMap[opr.ClientId] = opr.Seq
			kv.unlock("apply")
		}
		result.err = OK
	}

	return result
}

// the goroutine that listen to the snapshot checkpoint
func (kv *KVServer) snapshoter() {
	doneCh := kv.createDoneCh("snapshoter")
	defer kv.deleteDoneCh("snapshoter")

	ticker := time.NewTicker(SNAPSHOT_CHECKTIME)

	for {
		select {
		case <-ticker.C:

			// check whether we should snapshot
			if kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
				kv.snapshot()
			}

		case <-doneCh:

			ticker.Stop()

			return
		}
	}
}

// real logic of snapshot, encode state to []byte
func (kv *KVServer) snapshot() {
	kv.lock("snapshot")
	defer kv.unlock("snapshot")

	DPrintf("[%v] snapshoting, lastInclude is [%v]", kv.me, kv.LastIncludedIndex)

	buf := new(bytes.Buffer)

	encoder := gob.NewEncoder(buf)

	err := encoder.Encode(&kv.DB)

	if err != nil {
		panic(fmt.Sprintf("[%v] snapshot error", kv.me))
	}

	if encoder.Encode(&kv.DuplicateMap) != nil ||
		encoder.Encode(&kv.LastIncludedIndex) != nil {
		panic(fmt.Sprintf("[%v] snapshot error", kv.me))
	}

	kv.rf.Snapshot(kv.LastIncludedIndex, buf.Bytes())
}

func (kv *KVServer) restoreBySnapshot(snapshot []byte) {
	kv.lock("restoreBySnapshot")
	defer kv.unlock("restoreBySnapshot")

	decoder := gob.NewDecoder(bytes.NewBuffer(snapshot))

	if decoder.Decode(&kv.DB) != nil ||
		decoder.Decode(&kv.DuplicateMap) != nil ||
		decoder.Decode(&kv.LastIncludedIndex) != nil {

		panic(fmt.Sprintf("[%v] restore by snapshot error", kv.me))
	}
	DPrintf("[%v] restore lastApplied is [%v]", kv.me, kv.LastIncludedIndex)
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
