/* types.go
 Constants & structs used throughout the server code
 */
package copskv

import (
	"sync"

	"labrpc"
	"raft"
	"copsmaster"
)


const (
	OpRegisterClient uint8 = iota
	OpPutAfter        // PutAfter (client-facing & async)
	OpGetByVersion    // GetByVersion (client-facing)
	OpDepCheck        // DepCheck (async PutAfter)
	OpAsyncReplicated // to pop an op from toReplicate
	OpNeverDepend     // NeverDepend (garbage collection for fully replicated Puts)
	OpConfChangePrep  // Initiate shard config change
	OpConfChange      // Complete shard config change
)

type Op struct {
	Type uint8
	Key string
	Value string

	Version uint64
	Nearest map[string]uint64

	ClientID int64
	CommandID uint8
	ConfigNum int

	Config copsmaster.Config

	ShardStore map[int]map[string]Entry
	LastApplied map[int64]CmdResults
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	nodeID       uint32
	masters      []*labrpc.ClientEnd
	mck          *copsmaster.Clerk

	interConf    bool
	curConfig    copsmaster.Config
	nextConfig   copsmaster.Config
	accepted     [copsmaster.NShards]bool

	cid          int // cluster ID
	nclusters    int
	configs      []copsmaster.Config // Other clusters' configurations
	mcks         []*copsmaster.Clerk

	maxraftstate int // snapshot if log grows this big
	lastAppliedIndex int // log index of last applied command

	lastApplied map[int64]CmdResults
	kvstore [copsmaster.NShards]map[string]Entry // keep a separate k-v store for each shard

	latestTimestamp uint32 // The latest timestamp witnessed

	toReplicate  []Op
	asyncputmu   sync.Mutex
}

type Entry struct {
	Version uint64 // higher bits lamport timestamp, lower bits node ID (cluster + group + node)
	Value string
	Deps map[string]uint64 // key:value
	NeverDepend bool
}

type CmdResults struct {
	Cmd Op
	CommandID uint8
	Key string
	Value string
	Err Err

	Version uint64
	Deps map[string]uint64
	NeverDepend bool
}

type KVSnapshot struct {
	LastApplied map[int64]CmdResults
	KVStore [copsmaster.NShards]map[string]Entry
	LastAppliedIndex int

	InterConf bool
	CurConfig  copsmaster.Config
	NextConfig copsmaster.Config
	Accepted   [copsmaster.NShards]bool

	ToReplicate []Op
}
