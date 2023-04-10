package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

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
	database        sync.Map // the map that maintain the key/value pair
	doneChPool      map[string]chan interface{}
	putAppendChPool map[int64]chan interface{}
	lastApplied     int      // maintain the index that last applied
	duplicateMap    sync.Map // map that record each client's last request
}

type RequestRecord struct {
	seq   int
	value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%v] server doing get [%v]", kv.me, args.Key)

	requestRecord := kv.getRequestRecord(args.ClientId)
	if requestRecord != nil && args.Seq <= requestRecord.seq {
		reply.Err = OK
		reply.Value = requestRecord.value
		reply.CommitIndex = kv.rf.GetCommitIndex()

		DPrintf("[%v] server get repeated get [%v]", kv.me, args.Key)

		return
	}

	opr := Op{
		Method:   GET,
		Args:     []string{args.Key},
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	kv.rf.Start(opr)

	DPrintf("[%v] server doing get [%v], seq is [%v]", kv.me, args.Key, args.Seq)
	currentCommitIndex := kv.rf.GetCommitIndex()
	if args.CommitIndex <= currentCommitIndex && kv.getLastApplied() >= currentCommitIndex {
		v, ok := kv.database.Load(args.Key)
		// have no key
		if !ok {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value, _ = v.(string)
		}
		reply.CommitIndex = currentCommitIndex

		DPrintf("[%v] server success get [%v —— %v]", kv.me, args.Key, reply.Value)
		DPrintf("[%v] server lastApplied is [%v], commitIndex is [%v]", kv.me, kv.rf.GetLastApplied(), kv.rf.GetCommitIndex())

		return

	}
	cName := fmt.Sprintf("Get_%v", args.ClientId)
	doneCh := kv.createDoneCh(cName)
	defer kv.deleteDoneCh(cName)

	resCh := make(chan GetReply)

	// goroutine that waiting for CommitIndex or LastApplied catch up
	go func() {
		res := GetReply{}
		for !kv.killed() {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				res.Err = ErrWrongLeader
				res.Value = ""

				resCh <- res
				return
			}
			currentCommitIndex := kv.rf.GetCommitIndex()
			if args.CommitIndex <= currentCommitIndex && kv.getLastApplied() >= currentCommitIndex {

				kv.lock("Get")
				v, ok := kv.database.Load(args.Key)
				kv.unlock("Get")

				// have no key
				if !ok {
					res.Err = ErrNoKey
					res.Value = ""
				} else {
					res.Err = OK
					res.Value, _ = v.(string)
				}

				res.CommitIndex = currentCommitIndex
				resCh <- res

				DPrintf("[%v] server success get [%v ——— %v]  ", kv.me, args.Key, res.Value)
				DPrintf("[%v] server lastApplied is [%v], commitIndex is [%v]", kv.me, kv.rf.GetLastApplied(), kv.rf.GetCommitIndex())

				return
			}

			DPrintf("[%v] get [%v] waiting for commitIndex [%v]. lastApplied is [%v]", kv.me, args.Key, args.CommitIndex, kv.getLastApplied())
			DPrintf("[%v] raft commitIndex is [%v] ", kv.me, kv.rf.GetCommitIndex())

			time.Sleep(10 * time.Millisecond)
		}
	}()
	select {
	case res := <-resCh:
		reply.Err = res.Err
		reply.CommitIndex = res.CommitIndex
		reply.Value = res.Value
	case <-doneCh:
	}
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// repeated request
	requestRecord := kv.getRequestRecord(args.ClientId)
	if requestRecord != nil && args.Seq <= requestRecord.seq {
		DPrintf("[%v] record seq is [%v]", kv.me, requestRecord.seq)
		DPrintf("[%v] repeated put, seq is [%v]", kv.me, args.Seq)

		reply.CommitIndex = kv.rf.GetCommitIndex()
		reply.Err = OK
		return
	}

	DPrintf("[%v] server doing %v [%v : %v], seq is [%v]", kv.me, args.Op, args.Key, args.Value, args.Seq)
	opr := Op{
		Method:   args.Op,
		Args:     []string{args.Key, args.Value},
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	commitIndex, _, _ := kv.rf.Start(opr)

	// waiting for majority kvserver get this Op (has been committed)
	for !kv.killed() {

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		// keep waiting until commit
		currentCommitIndex := kv.rf.GetCommitIndex()
		if currentCommitIndex >= commitIndex {
			reply.CommitIndex = currentCommitIndex
			reply.Err = OK
			DPrintf("[%v] server success %v [%v : %v], commitIndex is [%v]", kv.me, args.Op, args.Key, args.Value, currentCommitIndex)

			return
		}

		time.Sleep(20 * time.Millisecond)
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.doneChPool = make(map[string]chan interface{})
	kv.putAppendChPool = make(map[int64]chan interface{})

	kv.lastApplied = 0

	go kv.handler()

	return kv
}

// handler that listen to applyCh and handle the command
func (kv *KVServer) handler() {
	doneCh := kv.createDoneCh("handler")
	defer kv.deleteDoneCh("handler")

	for {
		select {
		case applyMsg := <-kv.applyCh:

			if applyMsg.CommandValid {
				opr, _ := applyMsg.Command.(Op)

				// throw the repeated log entry
				if requestRecord := kv.getRequestRecord(opr.ClientId); requestRecord != nil && opr.Seq <= requestRecord.seq {
					break
				}

				key := opr.Args[0]

				getVal := ""

				//DPrintf("111")
				switch opr.Method {

				case GET:
					v, ok := kv.database.Load(key)
					if !ok {
						v = ""
					}
					getVal, _ = v.(string)
				case PUT:
					value := opr.Args[1]
					kv.database.Store(key, value)
				case APPEND:
					value := opr.Args[1]

					v, ok := kv.database.Load(key)

					strv, _ := v.(string)
					if !ok {
						kv.database.Store(key, value)
					} else {
						kv.database.Store(key, strv+value)
					}
				}

				kv.lock("handler")
				if applyMsg.CommandIndex > kv.lastApplied {
					kv.lastApplied = applyMsg.CommandIndex
				}
				kv.unlock("handler")

				kv.duplicateMap.Store(opr.ClientId, RequestRecord{
					seq:   opr.Seq,
					value: getVal,
				})

				DPrintf("[%v] server lastApplied is [%v], commitIndex is [%v]", kv.me, kv.lastApplied, kv.rf.GetCommitIndex())
			}
		case <-doneCh:
			return
		}
	}
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
		close(doneCh)
		delete(kv.doneChPool, name)
	}

	DPrintf("[%v] channel %v has been closed", kv.me, name)
}

func (kv *KVServer) createPutAppendCh(id int64) chan interface{} {
	kv.lock("createPutAppendCh")
	defer kv.unlock("createPutAppendCh")

	ch := make(chan interface{}, 1)
	kv.putAppendChPool[id] = ch

	return ch
}
func (kv *KVServer) deletePutAppendCh(id int64) {
	kv.lock("deletePutAppendCh")
	defer kv.unlock("deletePutAppendCh")

	ch := kv.putAppendChPool[id]

	if ch != nil {
		close(ch)
		delete(kv.putAppendChPool, id)
	}
	DPrintf("[%v] channel %v has been closed", kv.me, id)
}

func (kv *KVServer) getLastApplied() int {
	kv.lock("getLastApplied")
	defer kv.unlock("getLastApplied")

	return kv.lastApplied
}

func (kv *KVServer) getRequestRecord(clientId int64) *RequestRecord {
	v, ok := kv.duplicateMap.Load(clientId)
	if ok {
		record, _ := v.(RequestRecord)
		return &record
	}
	return nil
}
