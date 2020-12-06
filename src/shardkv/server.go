package shardkv

import (
	"log"
	"os"
	"sync"
	"time"

	"labgob"
	"labrpc"
	"raft"
	"shardmaster"
)


const Debug = 0
var logFile *os.File = nil

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		if logFile == nil {
			var err error
			logFile, err = os.OpenFile("debug-logs.txt", os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
			if err != nil {
				log.Fatal("error opening logFile", err)
			}
			log.SetOutput(logFile)
		}
		log.Printf(format, a...)
	}
	return
}


const (
	OpRegisterClient uint8 = iota
	OpPut
	OpAppend
	OpGet
)


type Op struct {
	Type uint8
	Key string
	Value string

	ClientID int64
	CommandID uint8
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	mck          *shardmaster.Clerk

	config       shardmaster.Config // Most recent config

	maxraftstate int // snapshot if log grows this big
	lastAppliedIndex int // log index of last applied command

	lastApplied map[int64]CmdResults
	kvstore map[string]string
}

type CmdResults struct {
	Cmd Op
	Value string
	Err Err
}

type KVSnapshot struct {
	LastApplied map[int64]CmdResults
	KVStore map[string]string
	LastAppliedIndex int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server %d-%d handling get\n", kv.gid, kv.me)
	// Make sure key is in shard
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("incorrect shard\n")
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		return
	}
	
	setWrongLeader := func() {
		DPrintf("ShardKV %d WrongLeader Get %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}
	
	lastAppliedMatch := func() bool {
		kv.mu.Lock()
		lastApplied, ok := kv.lastApplied[args.ClientID]
		kv.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			reply.Value = lastApplied.Value
			DPrintf("ShardKV %d successfully returning Get %d-%d\n", kv.me, args.ClientID, args.CommandID)
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		return Op { OpGet, args.Key, "", args.ClientID, args.CommandID }
	}
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server %d-%d handling putappend\n", kv.gid, kv.me)
	// Make sure key is in shard
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("incorrect shard\n")
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		return
	}

	setWrongLeader := func() {
		DPrintf("ShardKV %d WrongLeader PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}
	
	lastAppliedMatch := func() bool {
		kv.mu.Lock()
		lastApplied, ok := kv.lastApplied[args.ClientID]
		kv.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			DPrintf("ShardKV %d successfully returning PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		command := Op { 0, args.Key, args.Value, args.ClientID, args.CommandID }
		if args.Op == "Put" {
			command.Type = OpPut
		} else {
			command.Type = OpAppend
		}
		return command
	}
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
}

// Code shared by Get and PutAppend RPC handlers
func (kv *ShardKV) RPCHandler(setWrongLeader func(),
							   lastAppliedMatch func() bool,
							   createCommand func() Op) {
	// If not leader, return WrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		setWrongLeader()
		return
	}
	// If lastApplied has the command, return response
	if (lastAppliedMatch()) {
		return
	}
	// Construct Op struct for the received request
	command := createCommand()
	// Start consensus for the request
	_, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		setWrongLeader()
		return
	}
	// Read applyCh until 1st occurrence of the request
	for {
		// If matching command applied, return
		if (lastAppliedMatch()) {
			break
		}
		// Keep checking if still leader (for the same term)
		if curTerm, _ := kv.rf.GetState(); curTerm != term {
			setWrongLeader()
			break
		}
		// Yield lock to let background routine apply commands
		time.Sleep(time.Duration(1000000))
	}
}

// Perform a max 3-second read from applyChannel
func (kv *ShardKV) readApplyCh() (raft.ApplyMsg, bool) {
	select {
    case applyMsg := <-kv.applyCh:
		return applyMsg, true
	case <-time.After(time.Duration(3000000000)):
        return raft.ApplyMsg{}, false
    }
}

// Apply a committed command to local state
func (kv *ShardKV) apply(op Op) {
	switch op.Type {
	case OpGet:
		val, ok := kv.kvstore[op.Key]
		if ok {
			kv.lastApplied[op.ClientID] = CmdResults{ op, val, OK }
		} else {
			kv.lastApplied[op.ClientID] = CmdResults{ op, "", ErrNoKey }
		}
	case OpPut:
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			kv.kvstore[op.Key] = op.Value
			kv.lastApplied[op.ClientID] = CmdResults{ op, "", OK }
		}
	case OpAppend:
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			val, ok := kv.kvstore[op.Key]
			if ok {
				kv.kvstore[op.Key] = val + op.Value
				kv.lastApplied[op.ClientID] = CmdResults{ op, "", OK }
			} else {
				kv.lastApplied[op.ClientID] = CmdResults{ op, "", ErrNoKey }
			}
		}
	default:
	}
}

func (kv *ShardKV) applySnapshot(snapshot KVSnapshot) {
	kv.lastApplied = snapshot.LastApplied
	kv.kvstore = snapshot.KVStore
	kv.lastAppliedIndex = snapshot.LastAppliedIndex
}

// Periodically apply newly committed commands from applyCh
// Also check Raft state size & snapshot when size reaches maxraftstate
func (kv *ShardKV) backgroundWorker() {
	for {
		// If server has been killed, terminate this routine as well
		if !kv.rf.IsAlive() {
			return
		}
		// Read a commit from applyCh
		committed, ok := kv.readApplyCh()
		if !ok {
			continue
		}
		kv.mu.Lock()
		// Apply the newly committed command (regular or snapshot)
		if committed.CommandValid {
			kv.apply(committed.Command.(Op))
			kv.lastAppliedIndex = committed.CommandIndex
		} else {
			kv.applySnapshot(committed.Command.(KVSnapshot))
		}
		// Check if it's time for a snapshot
		if kv.rf.StateSizeLimitReached(kv.maxraftstate) {
			snapshot := KVSnapshot{ kv.lastApplied, kv.kvstore, kv.lastAppliedIndex }
			kv.rf.Snapshot(snapshot, kv.lastAppliedIndex)
		}
		kv.mu.Unlock()
	}
}

// Polls shardmaster for config changes every 100 milliseconds
func (kv *ShardKV) configPoller() {
	for {
		// If server has been killed, terminate this routine as well
		if !kv.rf.IsAlive() {
			return
		}
		// Query config
		DPrintf("querying config\n")
		newconfig := kv.mck.Query(-1)
		kv.mu.Lock()
		if newconfig.Num > kv.config.Num {
			// TODO: handle config update
			DPrintf("server %d-%d: config updated to %d\n", kv.gid, kv.me, newconfig.Num)
			kv.config = newconfig
		}
		kv.mu.Unlock()
		// Sleep for 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	DPrintf("creating shard %d server %d\n", gid, me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVSnapshot{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.lastAppliedIndex = 0
	kv.kvstore = make(map[string]string)
	kv.lastApplied = make(map[int64]CmdResults)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.mck.Query(-1)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.backgroundWorker()
	go kv.configPoller()
	
	DPrintf("shard %d server %d started. current config number %d\n", kv.gid, kv.me, kv.config.Num)

	return kv
}
