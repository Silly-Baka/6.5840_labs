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
	database     sync.Map // the map that maintain the key/value pair
	doneChPool   map[string]chan interface{}
	getChPool    map[int]chan GetReply
	duplicateMap sync.Map // map that record each client's last request
}

type RequestRecord struct {
	seq   int
	value string
	err   Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	record := kv.getRequestRecord(args.ClientId)
	if record != nil {
		DPrintf("[%v] preRequest seq is [%v]", kv.me, record.seq)
		// late request, will not be received
		if args.Seq < record.seq {
			reply.Err = ErrLateRequest
			return
		}
		// request that has been executed, send the result
		if args.Seq == record.seq {
			reply.Err = record.err
			reply.Value = record.value

			DPrintf("[%v] repeated get request, and return executed value", kv.me)
			return
		}
	}

	DPrintf("[%v] server doing get [%v], client is [%v] seq is [%v]", kv.me, args.Key, args.ClientId, args.Seq)

	opr := Op{
		Method:   GET,
		Args:     []string{args.Key},
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}
	commitIndex, _, _ := kv.rf.Start(opr)

	DPrintf("[%v] server doing get [%v], commitIndex is [%v]", kv.me, args.Key, kv.rf.GetCommitIndex())

	cname := fmt.Sprintf("Get_%v", args.ClientId)
	doneCh := kv.createDoneCh(cname)
	defer kv.deleteDoneCh(cname)

	getCh := kv.createGetCh(commitIndex)
	defer kv.createGetCh(commitIndex)

	timer := time.NewTimer(GET_TIMEOUT)

	select {

	case res := <-getCh:

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}

		reply.Err = res.Err
		reply.Value = res.Value

		return

	case <-timer.C:
		// timeout and retry
		reply.Err = ErrRetry
		return

	case <-doneCh:

		reply.Err = ErrWrongLeader

		return
	}

}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// throw the repeated request
	v, ok := kv.duplicateMap.Load(args.ClientId)
	if ok {
		record, _ := v.(RequestRecord)
		// late request, will not be received
		if args.Seq < record.seq {
			reply.Err = ErrLateRequest
			return
		}
		// request that has been executed, send the result
		if args.Seq == record.seq {
			reply.Err = record.err

			return
		}
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
	kv.getChPool = make(map[int]chan GetReply)

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

				DPrintf("[%v] server get applyMsg, client is [%v] seq is [%v]", kv.me, opr.ClientId, opr.Seq)
				// throw the repeated log
				requestRecord := kv.getRequestRecord(opr.ClientId)
				if requestRecord != nil && opr.Seq <= requestRecord.seq {

					break
				} else {
					// only apply when the Seq larger

					key := opr.Args[0]

					var err Err
					val := ""

					switch opr.Method {

					case GET:
						v, ok := kv.database.Load(key)
						if !ok {
							v = ""
							err = ErrNoKey
						} else {
							err = OK
						}
						val, _ = v.(string)

					case PUT:
						value := opr.Args[1]
						kv.database.Store(key, value)
						err = OK
					case APPEND:
						value := opr.Args[1]

						v, ok := kv.database.Load(key)

						strv, _ := v.(string)
						if !ok {
							kv.database.Store(key, value)
						} else {
							kv.database.Store(key, strv+value)
						}
						err = OK
					}

					kv.duplicateMap.Store(opr.ClientId, RequestRecord{
						seq:   opr.Seq,
						value: val,
						err:   err,
					})

					kv.lock("handler")
					ch, ok := kv.getChPool[applyMsg.CommandIndex]
					kv.unlock("handler")

					if ok {
						ch <- GetReply{
							Err:   err,
							Value: val,
						}
					}

				}
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

func (kv *KVServer) createGetCh(commitIndex int) chan GetReply {
	kv.lock("createGetCh")
	defer kv.unlock("createGetCh")

	ch := make(chan GetReply)
	kv.getChPool[commitIndex] = ch

	return ch
}

func (kv *KVServer) deleteGetCh(commitIndex int) {
	kv.lock("deleteGetCh")
	defer kv.unlock("deleteGetCh")

	ch := kv.getChPool[commitIndex]

	if ch != nil {
		close(ch)
		delete(kv.getChPool, commitIndex)
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

func (kv *KVServer) getRequestRecord(clientId int64) *RequestRecord {
	v, ok := kv.duplicateMap.Load(clientId)
	if ok {
		record, _ := v.(RequestRecord)
		return &record
	}
	return nil
}
