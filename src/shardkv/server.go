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


const Debug = 1
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
	OpConfigChange
)


type Op struct {
	Type uint8
	Key string
	Value string

	ClientID int64
	CommandID uint8

	ConfigNum int

	Config shardmaster.Config
	ShardStore map[int]map[string]string
	LastApplied map[int64]CmdResults
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
	accepted     [shardmaster.NShards]bool // all shards currently accepted
	// tomove       map[int][]moveShards // gid : shards to move to gid in each reconfig
	initiatedConfigChange bool
	// configChanging bool // true if a config change has been initiated & isn't complete
	// toreceive []int

	maxraftstate int // snapshot if log grows this big
	lastAppliedIndex int // log index of last applied command

	lastApplied map[int64]CmdResults
	kvstore [shardmaster.NShards]map[string]string // keep a separate k-v store for each shard
}

type moveShards struct {
	configNum int // config number corresponding to migration
	shards []int // shards to migrate from one gid to another
}

type CmdResults struct {
	Cmd Op
	CommandID uint8
	Key string
	Value string
	Err Err
}

type KVSnapshot struct {
	LastApplied map[int64]CmdResults
	KVStore [shardmaster.NShards]map[string]string
	LastAppliedIndex int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("server %d-%d handling get\n", kv.gid, kv.me)
	// If in the middle of a config change, hold
	kv.mu.Lock()
	for kv.initiatedConfigChange {
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}
	// Make sure key is in shard
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("incorrect shard\n")
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	
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
		return Op { OpGet, args.Key, "", args.ClientID, args.CommandID, kv.config.Num, shardmaster.Config{}, map[int]map[string]string{}, map[int64]CmdResults{} }
	}
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("server %d-%d handling putappend\n", kv.gid, kv.me)
	// If in the middle of a config change, hold
	kv.mu.Lock()
	for kv.initiatedConfigChange {
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}
	// Make sure key is in shard
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("incorrect shard\n")
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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
		command := Op { 0, args.Key, args.Value, args.ClientID, args.CommandID, kv.config.Num, shardmaster.Config{}, map[int]map[string]string{}, map[int64]CmdResults{} }
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

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrNotLeader
		kv.mu.Unlock()
		return
	}
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrWrongGroup
	} else if kv.config.Num == args.ConfigNum && !kv.initiatedConfigChange {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
		// Return a copy of the shard
		copyOfShard := make(map[string]string)
		for k, v := range kv.kvstore[args.Shard] {
			copyOfShard[k] = v
		}
		reply.Shard = copyOfShard
		// Return relevant lastApplied entries
		relevantLastApplied := make(map[int64]CmdResults)
		for clientID, cmdres := range kv.lastApplied {
			if key2shard(cmdres.Key) == args.Shard {
				relevantLastApplied[clientID] = cmdres
			}
		}
		reply.LastApplied = relevantLastApplied
	}
	kv.mu.Unlock()
}

// Perform a max 3-second read from applyChannel
func (kv *ShardKV) readApplyCh() (raft.ApplyMsg, bool) {
	DPrintf("%d-%d reading from channel\n", kv.gid, kv.me)
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
		shard := key2shard(op.Key)
		// If shard not accepted, return error
		if !kv.accepted[shard] {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		val, ok := kv.kvstore[shard][op.Key]
		if ok {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, val, OK }
		} else {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrNoKey }
		}
	case OpPut:
		shard := key2shard(op.Key)
		if !kv.accepted[shard] {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			kv.kvstore[shard][op.Key] = op.Value
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK }
		}
	case OpAppend:
		shard := key2shard(op.Key)
		// If shard not accepted, return error
		if !kv.accepted[shard] {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			val, ok := kv.kvstore[shard][op.Key]
			if ok {
				kv.kvstore[shard][op.Key] = val + op.Value
				kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK }
			} else {
				kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrNoKey }
			}
		}
	case OpConfigChange:
		kv.applyConfigChange(op.ConfigNum, op.ShardStore, op.LastApplied, op.Config)
	default:
		DPrintf("unrecognized operation\n")
	}
}

func (kv *ShardKV) applySnapshot(snapshot KVSnapshot) {
	DPrintf("applying snapshot\n")
	kv.lastApplied = snapshot.LastApplied
	kv.kvstore = snapshot.KVStore
	kv.lastAppliedIndex = snapshot.LastAppliedIndex
}

func (kv *ShardKV) applyConfigChange(configNum int, shardstore map[int]map[string]string, lastApplied map[int64]CmdResults, newConfig shardmaster.Config) {
	DPrintf("applying config change\n")
	// If config change already applied, return
	if kv.config.Num >= configNum {
		return
	}
	// Update kvstore & accepted
	for shard, store := range shardstore {
		kv.kvstore[shard] = store
		kv.accepted[shard] = true
	}
	// Update lastApplied
	for clientID, cmdres := range lastApplied {
		c, ok := kv.lastApplied[clientID]
		if !ok {
			// If there's no entry for clientID in kv.lastApplied, add
			kv.lastApplied[clientID] = cmdres
		} else if cmdres.CommandID > c.CommandID {
			// If the received entry is later than existing, overwrite
			kv.lastApplied[clientID] = cmdres
		}
	}
	// Reset kv.initiatedConfigChange
	kv.initiatedConfigChange = false
	// Update config
	DPrintf("aaa")
	kv.config = newConfig
	DPrintf("%d-%d finished applying config change to %d\n", kv.gid, kv.me, kv.config.Num)

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
		// DPrintf("%d-%d got a commit\n", kv.gid, kv.me)
		kv.mu.Lock()
		// DPrintf("%d-%d processing commit\n", kv.gid, kv.me)
		// Apply the newly committed command (regular or snapshot)
		if committed.CommandValid {
			kv.apply(committed.Command.(Op))
			// DPrintf("%d-%d applied commit\n", kv.gid, kv.me)
			kv.lastAppliedIndex = committed.CommandIndex
		} else {
			kv.applySnapshot(committed.Command.(KVSnapshot))
		}
		// Check if it's time for a snapshot
		if kv.rf.StateSizeLimitReached(kv.maxraftstate) {
			snapshot := KVSnapshot{ kv.lastApplied, kv.kvstore, kv.lastAppliedIndex }
			kv.rf.Snapshot(snapshot, kv.lastAppliedIndex)
		}
		// DPrintf("%d-%d done processing commit\n", kv.gid, kv.me)
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
		// If leader, check for config updates
		if _, isLeader := kv.rf.GetState(); isLeader && !kv.initiatedConfigChange {
			// Query config
			newestconfig := kv.mck.Query(-1)
			kv.mu.Lock()
			if newestconfig.Num > kv.config.Num && !kv.initiatedConfigChange {
				DPrintf("server %d-%d: config updated to %d\n", kv.gid, kv.me, newestconfig.Num)
				kv.configChange()
			}
			kv.mu.Unlock()
		}
		// Sleep for 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) configChange() {
	kv.initiatedConfigChange = true
	// Retrieve next config to change to
	newconfig := kv.mck.Query(kv.config.Num + 1)
	// Figure out shards to send & remove them from accepted
	for shard, accepted := range kv.accepted {
		if accepted {
			newgid := newconfig.Shards[shard]
			if newgid != kv.gid {
				// Remove shard from kv.accepted to stop processing requests for it
				kv.accepted[shard] = false
			}
		}
	}
	// Figure out shards to receive
	toreceive := []int{} // array of shards to receive to fully transition to next config
	for shard, gid := range newconfig.Shards {
		if gid == kv.gid && !kv.accepted[shard] {
			toreceive = append(toreceive, shard)
		}
	}
	// Request shards to be added to this group
	kv.retrieveShards(newconfig, toreceive)
}

func (kv *ShardKV) retrieveShards(newconfig shardmaster.Config, toreceive []int) {
	// DPrintf("%d-%d in retrieveShards\n", kv.gid, kv.me)
	// Start routines to request shards from other groups
	shards := make(map[int]map[string]string)
	lastApplied := make(map[int64]CmdResults)
	ntorecv := len(toreceive)
	for _, shard := range toreceive {
		go kv.retrieveShard(shard, &shards, &lastApplied, &ntorecv)
	}
	// Wait until all shards have been received
	for ntorecv > 0 {
		kv.mu.Unlock()
		// DPrintf("%d-%d sleeping\n", kv.gid, kv.me)
		time.Sleep(10 * time.Millisecond)
		// DPrintf("%d-%d woke up\n", kv.gid, kv.me, toreceive)
		if !kv.rf.IsAlive() {
			// DPrintf("%d-%d dead boi\n", kv.gid, kv.me)
			kv.mu.Lock()
			return
		}
		kv.mu.Lock()
	}
	// DPrintf("%d-%d out of loop\n", kv.gid, kv.me)
	// When all shards have been received, add state change to log
	cmd := Op{ OpConfigChange, "", "", 0, 0, kv.config.Num + 1, newconfig, shards, lastApplied }
	kv.mu.Unlock()
	_, _, leader := kv.rf.Start(cmd)
	kv.mu.Lock()
	if !leader {
		return
	}
}

func (kv *ShardKV) retrieveShard(shard int, shards *map[int]map[string]string, lastApplied *map[int64]CmdResults, ntorecv *int) {
	kv.mu.Lock()
	for *ntorecv > 0 {
		kv.mu.Unlock()
		if !kv.rf.IsAlive() {
			return
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		kv.mu.Lock()
		DPrintf("%d-%d retrieving shard %d\n", kv.gid, kv.me, shard)
		gid := kv.config.Shards[shard]
		// If there is no previous group with the shard, just ignore
		if gid == 0 {
			*ntorecv -= 1
			(*shards)[shard] = make(map[string]string)
			kv.mu.Unlock()
			return
		}
		DPrintf("%d-%d gid!=0\n", kv.gid, kv.me)
		// Else, get shard from previous group
		group := kv.config.Groups[gid]
		args := GetShardArgs{ kv.config.Num, shard }
		for i := 0; i < len(group); i = (i + 1) % len(group) {
			srv := kv.make_end(group[i])
			var reply GetShardReply
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.GetShard", &args, &reply)
			kv.mu.Lock()
			if ok && reply.Err == OK {
				*ntorecv -= 1
				(*shards)[shard] = reply.Shard
				for clientID, cmdres := range reply.LastApplied {
					c, ok := (*lastApplied)[clientID]
					if !ok {
						// If there's no entry for clientID in lastApplied, add
						(*lastApplied)[clientID] = cmdres
					} else if cmdres.CommandID > c.CommandID {
						// If the received entry is later than existing, overwrite
						(*lastApplied)[clientID] = cmdres
					}
				}
				kv.mu.Unlock()
				return
			}
		}
	}
	kv.mu.Unlock()
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
	
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvstore[i] = make(map[string]string)
		kv.accepted[i] = false
	}
	
	kv.lastApplied = make(map[int64]CmdResults)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.mck.Query(-1)
	kv.initiatedConfigChange = false

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.backgroundWorker()
	go kv.configPoller()
	
	DPrintf("shard %d server %d started. current config number %d\n", kv.gid, kv.me, kv.config.Num)

	return kv
}
