package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	requestCh  chan RequestFuture // the channel that maintain the order of requests from one client
	lastLeader int
	me         int64
	seq        int // the sequence of request

}

type RequestFuture struct {
	method     string
	args       []string
	responseCh chan interface{}
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.me = nrand()
	ck.requestCh = make(chan RequestFuture)
	ck.seq = 1

	go ck.requestHandler()

	return ck
}

// the goroutine that send and response request in order
func (ck *Clerk) requestHandler() {
	doneCh := make(chan interface{})
	defer close(doneCh)

	for {
		select {
		case future := <-ck.requestCh:
			switch future.method {
			case GET:
				args := GetArgs{
					Key:      future.args[0],
					ClientId: ck.me,
					Seq:      ck.seq,
				}

				// blocking and waiting for response
				reply := ck.doGet(&args)

				future.responseCh <- reply.Value
			default:
				// PUT or APPEND
				args := PutAppendArgs{
					Key:      future.args[0],
					Value:    future.args[1],
					Op:       future.method,
					ClientId: ck.me,
					Seq:      ck.seq,
				}

				// blocking and waiting for response
				reply := ck.doPutAppend(&args)

				future.responseCh <- reply
			}
			// incr the seq
			ck.seq++

		case <-doneCh:
			return
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {

	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     GET,
			args:       []string{key},
			responseCh: ch,
		}
	}()
	// waiting for resp
	select {

	case resp := <-ch:

		v, ok := resp.(string)
		if !ok {
			DPrintf("get key [%v] response error", key)
			return ""
		}
		return v
	}
}

func (ck *Clerk) doGet(args *GetArgs) *GetReply {

	key := args.Key
	shard := key2shard(key)

	for {
		gid := ck.config.Shards[shard]
		// ask for the gid
		if servers, ok := ck.config.Groups[gid]; ok {
			for _, svrName := range servers {
				svr := ck.make_end(svrName)

				reply := GetReply{}
				if ok := svr.Call("ShardKV.Get", args, &reply); ok {
					if reply.Err == OK || reply.Err == ErrNoKey {
						return &reply
					}
					// not responsible for this key
					if reply.Err == ErrWrongGroup {
						break
					}
					// no reply or wrongLeader, continue
					// todo: store the last leader of that group
					// 	....
					//  ....
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// no gid Group or Wrong Group（have no shard）
		// ask Controller for the latest Configuration
		ck.config = ck.sm.Query(-1)
	}
}

// shared by PUT and APPEND.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {

	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     op,
			args:       []string{key, value},
			responseCh: ch,
		}
	}()
	// waiting for resp
	select {
	case <-ch:
	}
}

func (ck *Clerk) doPutAppend(args *PutAppendArgs) *PutAppendReply {

	key := args.Key
	shard := key2shard(key)

	for {
		gid := ck.config.Shards[shard]
		// ask for the gid
		if servers, ok := ck.config.Groups[gid]; ok {
			for _, svrName := range servers {
				svr := ck.make_end(svrName)

				reply := PutAppendReply{}
				if ok := svr.Call("ShardKV.PutAppend", args, &reply); ok {
					if reply.Err == OK {
						return &reply
					}
					// not responsible for this key
					if reply.Err == ErrWrongGroup {
						break
					}
					// no reply or wrongLeader, continue
					// todo: store the last leader of that group
					// 	....
					//  ....
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// no gid Group or Wrong Group（have no shard）
		// ask Controller for the latest Configuration
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
