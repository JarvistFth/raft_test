package kvraft

const (
	OK          = "OK"
	ErrNoKey    = "ErrNoKey"
	WrongLeader = "WrongLeader"
	Timeout = "Timeout"
)

type Msg string

const (
	PutCmd 	 = "Put"
	GetCmd	 = "Get"
	AppendCmd   = "Append"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string

	ClientId int64
	RequestId int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	RequestID int64

}

type PutAppendReply struct {
	Msg Msg
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	RequestID int64
}

type GetReply struct {
	Msg   Msg
	Value string
}

func (o Op) Equal(op Op) bool {
	return o.Key == op.Key && o.Value == op.Value && o.OpType == op.OpType && o.ClientId == op.ClientId && o.RequestId == op.RequestId
}
