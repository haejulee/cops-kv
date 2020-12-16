/* common.go
 Structs & constants shared between client & server
 */
package copskv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotLeader  = "ErrNotLeader"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAfterArgs struct {
	Key   string
	Value string
	Nearest map[string]uint64
	Version uint64 // 0 for null
	
	ClientID  int64
	CommandID uint8

	ConfigNum int
}

type PutAfterReply struct {
	WrongLeader bool
	Err         Err

	Version     uint64
}

type GetByVersionArgs struct {
	Key string
	Version uint64 // 0 for latest
	
	ClientID  int64
	CommandID uint8

	ConfigNum int
}

type GetByVersionReply struct {
	WrongLeader bool
	Err         Err

	Value       string
	Version     uint64
	Deps        map[string]uint64
	NeverDepend bool
}
