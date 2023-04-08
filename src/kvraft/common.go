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
	Id int64 // only id
}

type PutAppendReply struct {
	Err         Err
	CommitIndex int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommitIndex int
}

type GetReply struct {
	Err   Err
	Value string
	// todo
	CommitIndex int
}
