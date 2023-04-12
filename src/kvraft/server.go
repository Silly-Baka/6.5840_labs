package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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
	db           *DataBase
	doneChPool   map[string]chan interface{}
	duplicateMap map[int64]int              // map that record each client's last Seq
	waitingChMap map[int]chan RequestRecord // the map that record all the goroutines that waiting for command executed
}

type DataBase struct {
	data map[string]string
}

type RequestRecord struct {
	value string
	err   Err
}

func (db *DataBase) Get(key string) (string, Err) {
	val, ok := db.data[key]
	if !ok {
		val = ""
		return val, ErrNoKey
	}
	return val, OK
}

func (db *DataBase) Put(key string, value string) {
	db.data[key] = value
}

func (db *DataBase) Append(key string, value string) {
	val, ok := db.data[key]
	if !ok {
		val = ""
	}
	val += value

	db.data[key] = val
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

	recordCh := kv.createRecordCh(commitIndex)
	defer kv.deleteRecordCh(commitIndex)

	timer := time.NewTimer(RETRY_TIMEOUT)

	select {

	case res := <-recordCh:

		DPrintf("[%v] rpc handler get the result", kv.me)
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
	recordCh := kv.createRecordCh(commitIndex)
	defer kv.deleteRecordCh(commitIndex)

	timer := time.NewTimer(RETRY_TIMEOUT)

	select {

	case res := <-recordCh:

		DPrintf("[%v] rpc handler get the result", kv.me)
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

	// waiting for majority kvserver get this Op (has been committed)
	//for !kv.killed() {
	//
	//	_, isLeader := kv.rf.GetState()
	//	if !isLeader {
	//		reply.Err = ErrWrongLeader
	//		return
	//	}
	//
	//	// keep waiting until commit
	//	currentCommitIndex := kv.rf.GetCommitIndex()
	//	if currentCommitIndex >= commitIndex {
	//		reply.Err = OK
	//		DPrintf("[%v] server success %v [%v : %v], commitIndex is [%v]", kv.me, args.Op, args.Key, args.Value, currentCommitIndex)
	//
	//		return
	//	}
	//
	//	time.Sleep(20 * time.Millisecond)
	//}
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.doneChPool = make(map[string]chan interface{})
	kv.duplicateMap = make(map[int64]int)
	kv.waitingChMap = make(map[int]chan RequestRecord)

	db := new(DataBase)
	db.data = make(map[string]string)
	kv.db = db

	go kv.handler()

	return kv
}

// handler that listen to applyCh and handle the command
func (kv *KVServer) handler() {
	doneCh := kv.createDoneCh("handler")
	defer kv.deleteDoneCh("handler")

	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:

			if applyMsg.CommandValid {
				opr, _ := applyMsg.Command.(Op)

				res := kv.apply(opr)

				kv.lock("handler")
				ch, ok := kv.waitingChMap[applyMsg.CommandIndex]
				kv.unlock("handler")
				if ok {
					if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
						break
					}
					defer func() {
						if r := recover(); r != nil {
							DPrintf("[%v] send on closed channel", kv.me)
						}
					}()
					ch <- res
				}
			}
		case <-doneCh:

			DPrintf("[%v] server handler is dead", kv.me)
			return
		}
	}
	DPrintf("[%v] server handler is dead", kv.me)
}

func (kv *KVServer) apply(opr Op) RequestRecord {
	// check if outdated
	maxSeq := kv.duplicateMap[opr.ClientId]

	record := RequestRecord{}

	switch opr.Method {
	case GET:
		v, err := kv.db.Get(opr.Args[0])
		record.err = err
		record.value = v
	case PUT:

		if opr.Seq > maxSeq {
			kv.db.Put(opr.Args[0], opr.Args[1])
			kv.duplicateMap[opr.ClientId] = opr.Seq
		}
		record.err = OK
	case APPEND:
		if opr.Seq > maxSeq {
			kv.db.Append(opr.Args[0], opr.Args[1])
			kv.duplicateMap[opr.ClientId] = opr.Seq
		}
		record.err = OK
	}

	return record
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

func (kv *KVServer) createRecordCh(commitIndex int) chan RequestRecord {
	kv.lock("createRecordCh")
	defer kv.unlock("createRecordCh")

	ch := make(chan RequestRecord)
	kv.waitingChMap[commitIndex] = ch

	return ch
}

func (kv *KVServer) deleteRecordCh(commitIndex int) {
	kv.lock("deleteRecordCh")
	defer kv.unlock("deleteRecordCh")

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

//
//func (kv *KVServer) getRequestRecord(clientId int64) *RequestRecord {
//	//v, ok := kv.duplicateMap.Load(clientId)
//	//if ok {
//	//	record, _ := v.(RequestRecord)
//	//	return &record
//	//}
//	//return nil
//	kv.lock("getRequestRecord")
//	record, ok := kv.duplicateMap[clientId]
//	kv.unlock("getRequestRecord")
//
//	if ok {
//		return &record
//	} else {
//		return nil
//	}
//}
