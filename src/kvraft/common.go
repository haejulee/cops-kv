package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int
	CommandID uint8
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	ClientID  int
	CommandID uint8
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type RegisterClientArgs struct {}

type RegisterClientReply struct {
	ClientID	int
}
