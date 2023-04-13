package kvraft

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd

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

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	// get unique Id
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
				reply := ck.SendGet(&args)

				future.responseCh <- reply.Value
			default:
				// Put or Append
				args := PutAppendArgs{
					Key:      future.args[0],
					Value:    future.args[1],
					Op:       future.method,
					ClientId: ck.me,
					Seq:      ck.seq,
				}

				// blocking and waiting for response
				reply := ck.SendPutAppend(&args)

				future.responseCh <- reply
			}

		case <-doneCh:
			return
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &Args, &reply)
//
// the types of Args and reply (including whether they are pointers)
// must match the declared types of the RPC applier function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     GET,
			args:       []string{key},
			responseCh: ch,
		}
	}()

	// waiting for response
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

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &Args, &reply)
//
// the types of Args and reply (including whether they are pointers)
// must match the declared types of the RPC applier function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

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

func (ck *Clerk) SendGet(args *GetArgs) *GetReply {

	// send to i first
	i := ck.lastLeader

	l := len(ck.servers)

	// retry until get value successfully
	for {
		DPrintf("client call get [%v] to server [%v]", args.Key, i)
		reply := GetReply{}
		if ok := ck.servers[i].Call("KVServer.Get", args, &reply); ok {
			// have no err means that get value successfully
			if reply.Err == OK || reply.Err == ErrNoKey {

				ck.lastLeader = i
				ck.seq++

				DPrintf("client success get [%v]", args.Key)

				return &reply
			}
		}
		// retry, maybe timeout or wrong leader
		i = (i + 1) % l
	}
}
func (ck *Clerk) SendPutAppend(args *PutAppendArgs) *PutAppendReply {
	// send to lastLeader first
	i := ck.lastLeader

	l := len(ck.servers)

	// retry until get value successfully
	for {
		DPrintf("client sending putAppend [%v:%v] to server [%v]", args.Key, args.Value, i)
		reply := PutAppendReply{}
		if ok := ck.servers[i].Call("KVServer.PutAppend", args, &reply); ok {
			// have no err means that get value successfully
			if reply.Err == OK {

				ck.lastLeader = i
				ck.seq++

				DPrintf("client success putAppend [%v]", args.Key)

				return &reply
			}
		}
		i = (i + 1) % l
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
