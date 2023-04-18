package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	requestCh  chan RequestFuture // the channel that maintain the order of requests from one client
	lastLeader int
	me         int64
	seq        int // the sequence of request
}

// RequestFuture the struct that used to receive result async /*
type RequestFuture struct {
	method     string
	args       []interface{}
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
	// Your code here.

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
			case Join:
				servers, _ := future.args[0].(map[int][]string)
				args := JoinArgs{
					Servers:  servers,
					ClientId: ck.me,
					Seq:      ck.seq,
				}
				ck.doJoin(&args)

				future.responseCh <- struct{}{}

			case Leave:
				gids, _ := future.args[0].([]int)
				args := LeaveArgs{
					GIDs:     gids,
					ClientId: ck.me,
					Seq:      ck.seq,
				}
				ck.doLeave(&args)

				future.responseCh <- struct{}{}

			case Move:
				shard, _ := future.args[0].(int)
				gid, _ := future.args[1].(int)
				args := MoveArgs{
					Shard:    shard,
					GID:      gid,
					ClientId: ck.me,
					Seq:      ck.seq,
				}
				ck.doMove(&args)

			case Query:

				num, _ := future.args[0].(int)
				args := QueryArgs{
					Num:      num,
					ClientId: ck.me,
					Seq:      ck.seq,
				}

				future.responseCh <- ck.doQuery(&args)
			}

		case <-doneCh:
			return
		}
	}
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     Query,
			args:       []interface{}{num},
			responseCh: ch,
		}
	}()

	// waiting for response
	select {
	case resp := <-ch:

		v, _ := resp.(Config)
		return v
	}
}

func (ck *Clerk) doQuery(args *QueryArgs) *Config {
	// try each known server.

	i := ck.lastLeader
	l := len(ck.servers)

	for {
		DPrintf("client call Query [%v] to server [%v]", args.Num, i)
		reply := QueryReply{}
		if ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply); ok {
			// have no err means that get value successfully
			if reply.Err == OK {

				ck.lastLeader = i
				ck.seq++

				DPrintf("client success Query [%v]", args.Num)

				return &reply.Config
			}
		}
		// retry, maybe timeout or wrong leader
		i = (i + 1) % l
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     Join,
			args:       []interface{}{servers},
			responseCh: ch,
		}
	}()

	// waiting for response
	select {
	case <-ch:
		return
	}
}

func (ck *Clerk) doJoin(args *JoinArgs) {
	i := ck.lastLeader
	l := len(ck.servers)

	for {
		DPrintf("client call Join [%v] to server [%v]", args.Servers, i)
		reply := JoinReply{}
		if ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply); ok {
			// have no err means that get value successfully
			if reply.Err == OK {

				ck.lastLeader = i
				ck.seq++

				DPrintf("client success Join [%v]", args.Servers)

				return
			}
		}
		// retry, maybe timeout or wrong leader
		i = (i + 1) % l
	}
}

func (ck *Clerk) Leave(gids []int) {
	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     Leave,
			args:       []interface{}{gids},
			responseCh: ch,
		}
	}()

	// waiting for response
	select {
	case <-ch:
		return
	}
}

func (ck *Clerk) doLeave(args *LeaveArgs) {
	i := ck.lastLeader
	l := len(ck.servers)

	for {
		DPrintf("client call Leave [%v] to server [%v]", args.GIDs, i)
		reply := LeaveReply{}
		if ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply); ok {
			// have no err means that get value successfully
			if reply.Err == OK {

				ck.lastLeader = i
				ck.seq++

				DPrintf("client success Leave [%v]", args.GIDs)

				return
			}
		}
		// retry, maybe timeout or wrong leader
		i = (i + 1) % l
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ch := make(chan interface{})

	go func() {
		ck.requestCh <- RequestFuture{
			method:     Move,
			args:       []interface{}{shard, gid},
			responseCh: ch,
		}
	}()

	// waiting for response
	select {
	case <-ch:
		return
	}
}

func (ck *Clerk) doMove(args *MoveArgs) {
	i := ck.lastLeader
	l := len(ck.servers)

	for {
		DPrintf("client call Move [%v to %v] to server [%v]", args.Shard, args.GID, i)
		reply := MoveReply{}
		if ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply); ok {
			// have no err means that get value successfully
			if reply.Err == OK {

				ck.lastLeader = i
				ck.seq++

				DPrintf("client success Move [%v to %v]", args.Shard, args.GID)

				return
			}
		}
		// retry, maybe timeout or wrong leader
		i = (i + 1) % l
	}
}
