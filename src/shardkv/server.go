package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"math/rand"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method   string   // get、put、append
	Args     []string // Args[0] = key , Args[1] = value
	ClientId int64
	Seq      int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 3A
	DB            *DataBase
	doneChPool    map[string]chan interface{}
	DuplicateMap  map[int64]int               // map that record each client's last Seq
	waitingChPool map[int]chan *RequestResult // the map that record all the goroutines that waiting for command executed

	// 4B
	config   *shardctrler.Config // the relative latest config from Shard Controller（maybe late）
	ctlerclk *shardctrler.Clerk  // clerk that used to call Shards Controller
}

type DataBase struct {
	Data map[string]string
}

type RequestResult struct {
	value string
	err   Err
}

func NewDataBase() *DataBase {
	db := DataBase{}

	db.Data = make(map[string]string)

	return &db
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// todo: do not resolve when reconfiguring

	config := kv.getConfig()

	// return if invalid config
	if config.Num == 0 {
		reply.Err = ErrWrongGroup
		return
	}

	shard := key2shard(args.Key)

	// not responsible for this shard，do not resolve
	if config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

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
	//realNextIndex := kv.rf.GetRealNextIndex()
	//DPrintf("[%v] server doing get [%v], commandIndex is [%v], realNextIndex is [%v]", kv.me, args.Key, commandIndex, realNextIndex)

	waitingCh := kv.createWaitingCh(commandIndex)

	timer := time.NewTimer(getRetryTimeout())

	select {

	case res := <-waitingCh:

		// close the channel
		kv.deleteWaitingCh(commandIndex)

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
		timer.Stop()

		// the goroutine that waiting for result
		go func() {
			defer kv.deleteWaitingCh(commandIndex)

			<-waitingCh
		}()

		return
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	config := kv.getConfig()

	// return if invalid config
	if config.Num == 0 {
		reply.Err = ErrWrongGroup
		return
	}

	shard := key2shard(args.Key)

	// not responsible for this shard，do not resolve
	if config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

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

	DPrintf("[%v] server doing %v [%v : %v], seq is [%v], CommandIndex is [%v]", kv.me, args.Op, args.Key, args.Value, args.Seq, commandIndex)

	waitingCh := kv.createWaitingCh(commandIndex)

	timer := time.NewTimer(getRetryTimeout())

	select {

	case res := <-waitingCh:

		kv.deleteWaitingCh(commandIndex)

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
		timer.Stop()

		go func() {
			defer kv.deleteWaitingCh(commandIndex)

			<-waitingCh
		}()

		return
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.

	kv.lock("Kill")
	defer kv.unlock("Kill")

	for _, ch := range kv.doneChPool {
		ch <- struct{}{}
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	// 4B
	kv.ctlerclk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// invalid config
	kv.config = &shardctrler.Config{
		Num: 0,
	}
	kv.doneChPool = make(map[string]chan interface{})
	kv.waitingChPool = make(map[int]chan *RequestResult)
	kv.DuplicateMap = make(map[int64]int)
	kv.DB = NewDataBase()

	// backup configurer that listen to the change of configuration
	go kv.configurer()
	go kv.applier()

	return kv
}

// the goroutine that ask for latest Configuration periodically
func (kv *ShardKV) configurer() {

	doneCh := kv.createDoneCh("configurer")
	defer kv.deleteDoneCh("configurer")

	timer := time.NewTimer(ConfigureInterval)

	for {
		select {
		case <-timer.C:
			kv.lock("configurer")

			config := kv.ctlerclk.Query(-1)
			kv.config = &config

			DPrintf("[%v] configurer get new configuration [%v]", kv.me, config)
			kv.unlock("configurer")

			timer.Reset(ConfigureInterval)

		case <-doneCh:
			return
		}
	}
}

// the goroutine that handle the applyMsg
func (kv *ShardKV) applier() {
	doneCh := kv.createDoneCh("applier")
	defer kv.deleteDoneCh("applier")

	for {
		select {

		case applyMsg := <-kv.applyCh:

			if applyMsg.CommandValid {
				DPrintf("[%v] get applyMsg index is [%v]", kv.me, applyMsg.CommandIndex)

				opr := applyMsg.Command.(Op)

				res := kv.apply(opr)

				DPrintf("[%v] success handler opr [%v]", kv.me, opr)

				kv.lock("applier")
				ch, ok := kv.waitingChPool[applyMsg.CommandIndex]
				kv.unlock("applier")

				if ok {
					if term, isLeader := kv.rf.GetState(); !isLeader || term != applyMsg.CommandTerm {
						break
					}
					//DPrintf("[%v] sending res to chan [%v]", kv.me, applyMsg.CommandIndex)
					ch <- res
					//DPrintf("[%v] success send result res to chan [%v]", kv.me, applyMsg.CommandIndex)
				}
			}

		case <-doneCh:
			return
		}
	}
}

// the real logic of apply
func (kv *ShardKV) apply(opr Op) *RequestResult {
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

	return &result
}

func (kv *ShardKV) getConfig() *shardctrler.Config {
	kv.lock("getConfig")
	defer kv.unlock("getConfig")

	return kv.config
}

func (kv *ShardKV) createWaitingCh(commitIndex int) chan *RequestResult {
	kv.lock("createWaitingCh")
	defer kv.unlock("createWaitingCh")

	ch := make(chan *RequestResult)
	kv.waitingChPool[commitIndex] = ch

	return ch
}

func (kv *ShardKV) deleteWaitingCh(commitIndex int) {
	kv.lock("deleteWaitingCh")
	defer kv.unlock("deleteWaitingCh")

	ch := kv.waitingChPool[commitIndex]

	if ch != nil {
		delete(kv.waitingChPool, commitIndex)
		close(ch)
	}
}

func (kv *ShardKV) createDoneCh(name string) <-chan interface{} {
	//rf.mu.Lock()
	kv.lock("createDoneCh")
	defer kv.unlock("createDoneCh")

	DPrintf("[%v] channel %v has been created", kv.me, name)

	doneCh := make(chan interface{}, 1)
	kv.doneChPool[name] = doneCh

	return doneCh
}

func (kv *ShardKV) deleteDoneCh(name string) {
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

func (kv *ShardKV) lock(name string) {
	kv.mu.Lock()

	DPrintf("[%v] function [%v()] get this lock", kv.me, name)
}
func (kv *ShardKV) unlock(name string) {
	kv.mu.Unlock()

	DPrintf("[%v] function [%v()] unlock", kv.me, name)
}

func getRetryTimeout() time.Duration {

	ms := 500 + (rand.Int63() % 500)

	return time.Duration(ms) * time.Millisecond
}
