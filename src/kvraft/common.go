package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	CkLock         = false
	KvLock         = true

	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	Err         Err
	CommitIndex int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommitIndex int
	ClientId    int64
	Seq         int
}

type GetReply struct {
	Err         Err
	Value       string
	CommitIndex int
}
