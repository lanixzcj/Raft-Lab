package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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
	UUID string
}

type PutAppendReply struct {
	WrongLeader bool
	LeaderId    int
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID string
}

type GetReply struct {
	WrongLeader bool
	LeaderId    int
	Err         Err
	Value       string
}
