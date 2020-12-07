package copskv

import (
	"log"
	"os"
	"sync"
	"time"

	"labgob"
	"labrpc"
	"raft"
	"copsmaster"
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
	OpPutAfter
	OpGetByVersion
	OpDepCheck
	OpConfigChange
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

	config       copsmaster.Config // Most recent config
	accepted     [copsmaster.NShards]bool // all shards currently accepted
	// tomove       map[int][]moveShards // gid : shards to move to gid in each reconfig
	initiatedConfigChange bool
	// configChanging bool // true if a config change has been initiated & isn't complete
	// toreceive []int

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

	Version uint64
	Deps map[string]uint64
	NeverDepend bool
}

type KVSnapshot struct {
	LastApplied map[int64]CmdResults
	KVStore [copsmaster.NShards]map[string]Entry
	LastAppliedIndex int
}

// Extracts just the Lamport Timestamp from a version number
func vtot(versionNum uint64) uint32 {
	return uint32(versionNum >> 32)
}

// Returns true if v1 is at least as late as v2
func versionUpToDate(v1, v2 uint64) bool {
	// Convert to lamport timestamps
	t1 := vtot(v1)
	t2 := vtot(v2)
	// compare timestamps
	if t1 >= t2 {
		return true
	} else {
		return false
	}
}

// Returns Lamport timestamp of next event
// Must hold lock while calling
func (kv *ShardKV) lamportTimestamp() uint32 {
	// Add 1 to latest timestamp & return its value
	kv.latestTimestamp += 1
	return kv.latestTimestamp
}

// Updates system's latest witnessed timestamp,
// considering an incoming timestamp
// Must hold lock while calling
func (kv *ShardKV) updateTimestamp(incoming uint32) {
	if incoming > kv.latestTimestamp {
		kv.latestTimestamp = incoming
	}
}

// Returns a next version number based on Lamport Timestamp
// Must hold lock while calling
func (kv *ShardKV) versionNumber() uint64 {
	timestamp := uint64(kv.lamportTimestamp())
	ver := timestamp << 32
	ver = ver | uint64(kv.nodeID)
	return ver
}

func (kv *ShardKV) GetByVersion(args *GetByVersionArgs, reply *GetByVersionReply) {
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
			reply.Version = lastApplied.Version
			reply.Deps = lastApplied.Deps
			reply.NeverDepend = lastApplied.NeverDepend
			if reply.Err == OK {
				DPrintf("%d-%d successfully returning Get %d-%d\n", kv.gid, kv.me, args.ClientID, args.CommandID)
			} else {
				DPrintf("%d-%d error returning Get %d-%d\n", kv.gid, kv.me, args.ClientID, args.CommandID)
			}
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		return Op { OpGetByVersion, args.Key, "", args.Version, map[string]uint64{}, args.ClientID, args.CommandID,
		            kv.config.Num, copsmaster.Config{}, map[int]map[string]Entry{}, map[int64]CmdResults{} }
	}
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
}

func (kv *ShardKV) PutAfterHandler(args *PutAfterArgs, reply *PutAfterReply) {
	DPrintf("server %d-%d handling putappend\n", kv.gid, kv.me)
	// If in the middle of a config change, hold
	kv.mu.Lock()
	for kv.initiatedConfigChange {
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
	}
	DPrintf("QQQ")
	// Make sure key is in shard
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		DPrintf("incorrect shard\n")
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	DPrintf("WWW")

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
			reply.Version = lastApplied.Version
			DPrintf("ShardKV %d successfully returning PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		command := Op { OpPutAfter, args.Key, args.Value, args.Version, args.Nearest, args.ClientID, args.CommandID,
			            kv.config.Num, copsmaster.Config{}, map[int]map[string]Entry{}, map[int64]CmdResults{} }
		return command
	}

	DPrintf("EEE")
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
}

func (kv *ShardKV) PutAfter(args *PutAfterArgs, reply *PutAfterReply) {
	// If version is nil, synchronous put_after -> immediately commit put
	if args.Version == 0 {
		kv.PutAfterHandler(args, reply)
		return
	}
	// Else, handle async put_after -> commit after dependencies met
	DPrintf("%d-%d-%d Received async PutAfter\n", kv.cid, kv.gid, kv.me)

	// If not leader, return
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
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
	
	// Make sure dependencies are met
	DPrintf("%d-%d-%d Doing dependency checks\n", kv.cid, kv.gid, kv.me)
	kv.mu.Unlock()
	kv.doDepChecks(args.Nearest)
	
	// Once dependencies are met, commit put
	DPrintf("%d-%d-%d Committing async PutAfter\n", kv.cid, kv.gid, kv.me)
	kv.asyncputmu.Lock()
	kv.PutAfterHandler(args, reply)
	kv.asyncputmu.Unlock()
	DPrintf("%d-%d-%d Returning async PutAfter\n", kv.cid, kv.gid, kv.me)
}

// Code shared by GetByVersion and PutAfter RPC handlers
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
	DPrintf("RRR")
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
		DPrintf("%d-%d-%d PPP", kv.cid, kv.gid, kv.me)
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) DepCheck(args *DepCheckArgs, reply *DepCheckReply) {
	// TODO: make sure node is the primary of key
	// Make sure leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	// Do dependency check
	reply.WrongLeader = false
	// TODO: maybe make this check blocking until satisfied, as long as still leader
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	entry := kv.kvstore[shard][args.Key]
	if versionUpToDate(entry.Version, args.Version) {
		reply.Ok = true
	} else {
		reply.Ok = false
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
		copyOfShard := make(map[string]Entry)
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
	// DPrintf("%d-%d reading from channel\n", kv.gid, kv.me)
	select {
    case applyMsg := <-kv.applyCh:
		return applyMsg, true
	case <-time.After(time.Duration(3000000000)):
        return raft.ApplyMsg{}, false
    }
}

// Apply a committed command to local state
func (kv *ShardKV) apply(op Op) {
	DPrintf("%d-%d-%d applying operation", kv.cid, kv.gid, kv.me)
	switch op.Type {
	case OpGetByVersion:
		shard := key2shard(op.Key)
		// If shard not accepted, return error
		if !kv.accepted[shard] {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup, 0, map[string]uint64{}, false }
			return
		}
		entry, ok := kv.kvstore[shard][op.Key]
		if ok {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, entry.Value, OK, entry.Version, entry.Deps, entry.NeverDepend }
		} else {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrNoKey, 0, map[string]uint64{}, false }
		}
	case OpPutAfter:
		shard := key2shard(op.Key)
		if !kv.accepted[shard] {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup, 0, map[string]uint64{}, false }
			return
		}
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			// Determining version of put operation
			version := op.Version
			if version == 0 {
				// If version argument was nil, generate new version number & apply immediately
				version = kv.versionNumber()
				kv.kvstore[shard][op.Key] = Entry{ version, op.Value, op.Nearest, false }
				kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK, version, op.Nearest, false }
				// Append put operation with the generated version number to replication queue
				DPrintf("%d-%d-%d adding to toReplicate", kv.cid, kv.gid, kv.me)
				op.Version = version
				kv.toReplicate = append(kv.toReplicate, op)
			} else {
				// If version specified,
				// update latest timestamp using given version
				kv.updateTimestamp(vtot(version))
				// Apply put operation, only if new version is higher than existing
				entry, ok := kv.kvstore[shard][op.Key]
				DPrintf("async put %s:%s", op.Key, op.Value)
				if !ok || vtot(entry.Version) < vtot(version) ||
					(vtot(entry.Version) == vtot(version) && uint32(entry.Version) < uint32(version)) {
					DPrintf("writing async put %s:%s", op.Key, op.Value)
					kv.kvstore[shard][op.Key] = Entry{ version, op.Value, op.Nearest, false }
					kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK, version, op.Nearest, false }
				}
				DPrintf("CCC")
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

func (kv *ShardKV) applyConfigChange(configNum int, shardstore map[int]map[string]Entry, lastApplied map[int64]CmdResults, newConfig copsmaster.Config) {
	DPrintf("aa\n")
	// If config change already applied, return
	if kv.config.Num >= configNum {
		return
	}
	DPrintf("%d-%d applying config change\n", kv.gid, kv.me)
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
	kv.config = newConfig
	DPrintf("%d-%d config changed to %d\n", kv.gid, kv.me, kv.config.Num)
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

// Asynchronously replicates put operations
func (kv *ShardKV) replicationWorker() {
	for {
		// If server has been killed, terminate this routine as well
		if !kv.rf.IsAlive() {
			return
		}
		// Check if there are any puts to replicate across wide-area
		kv.mu.Lock()
		if len(kv.toReplicate) > 0 {
			// Pop first operation from queue
			op := kv.toReplicate[0]
			DPrintf("%d-%d-%d replicating op", kv.cid, kv.gid, kv.me, op)
			if len(kv.toReplicate) == 1 {
				kv.toReplicate = []Op{}
			} else {
				kv.toReplicate = append([]Op{}, kv.toReplicate[1:]...)
			}
			// Replicate operation to other clusters
			kv.mu.Unlock()
			mu := sync.Mutex{}
			count := 0
			mu.Lock()
			DPrintf("XXX nclusters %d", kv.nclusters, kv.mcks)
			for c := 0; c < kv.nclusters; c++ {
				// Iterate through each other cluster
				if c != kv.cid {
					// Find out latest configuration of other cluster
					latestConf := kv.mcks[c].Query(-1)
					// Figure out group in charge of key
					shard := key2shard(op.Key)
					gid := latestConf.Shards[shard]
					group := latestConf.Groups[gid]
					// Send PutAfter request to group until success
					count += 1
					go kv.replicateToCluster(op, group, &count, &mu)
				}
			}
			// Wait until all replications are done
			for count > 0 {
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				mu.Lock()
			}
		} else {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) replicateToCluster(op Op, group []string, count *int, mu *sync.Mutex) {
	DPrintf("%d-%d-%d replicating op to other cluster", kv.cid, kv.gid, kv.me, op)
	args := PutAfterArgs{ op.Key, op.Value, op.Nearest, op.Version, op.ClientID, op.CommandID }
	for i := 0; ; {
		srv := kv.make_end(group[i])
		var reply PutAfterReply
		ok := srv.Call("ShardKV.PutAfter", &args, &reply)
		if !ok || reply.WrongLeader {
			if ok {
				DPrintf("replicateToCluster: received wrong leader")
			} else {
				DPrintf("replicateToCluster: network error")
			}
			i = (i + 1) % len(group)
			time.Sleep(time.Millisecond)
			continue
		}
		if reply.Err == OK {
			DPrintf("replicateToCluster: success")
			mu.Lock()
			*count -= 1
			mu.Unlock()
			return
		}
		DPrintf("replicateToCluster: received error")
		time.Sleep(time.Millisecond)
	}
}

// Polls copsmaster for config changes every 100 milliseconds
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

func (kv *ShardKV) retrieveShards(newconfig copsmaster.Config, toreceive []int) {
	// DPrintf("%d-%d in retrieveShards\n", kv.gid, kv.me)
	// Start routines to request shards from other groups
	shards := make(map[int]map[string]Entry)
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
	cmd := Op{ OpConfigChange, "", "", 0, map[string]uint64{}, 0, 0,
	           kv.config.Num + 1, newconfig, shards, lastApplied }
	kv.mu.Unlock()
	_, _, leader := kv.rf.Start(cmd)
	kv.mu.Lock()
	if !leader {
		return
	}
}

func (kv *ShardKV) retrieveShard(shard int, shards *map[int]map[string]Entry, lastApplied *map[int64]CmdResults, ntorecv *int) {
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
		// DPrintf("%d-%d retrieving shard %d\n", kv.gid, kv.me, shard)
		gid := kv.config.Shards[shard]
		// If there is no previous group with the shard, just ignore
		if gid == 0 {
			*ntorecv -= 1
			(*shards)[shard] = make(map[string]Entry)
			kv.mu.Unlock()
			return
		}
		// DPrintf("%d-%d gid!=0\n", kv.gid, kv.me)
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

// Blocks until all dependencies in deps are satisfied in the cluster
func (kv *ShardKV) doDepChecks(deps map[string]uint64) {
	mu := sync.Mutex{}
	count := 0
	// Carry out dependency checks for each key concurrently
	mu.Lock()
	for key, version := range deps {
		count += 1
		go kv.doDepCheck(key, version, &mu, &count)
	}
	// Wait until all checks are done
	for count > 0 {
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
	}
}

func (kv *ShardKV) doDepCheck(key string, version uint64, mu *sync.Mutex, count *int) {
	// Find group (& its servers) in charge of key
	shard := key2shard(key)
	latestConfig := kv.mck.Query(-1)
	gid := latestConfig.Shards[shard]
	servers := latestConfig.Groups[gid]
	// Query servers in charge until success
	for i := 0; ; {
		srv := kv.make_end(servers[i])
		args := DepCheckArgs{ key, version }
		var reply DepCheckReply
		ok := srv.Call("ShardKV.DepCheck", &args, &reply)
		if !ok || reply.WrongLeader {
			// If wrong leader or connection issue, try next server
			i = (i+1) % len(servers)
			continue
		}
		if reply.Ok {
			mu.Lock()
			*count -= 1
			mu.Unlock()
			return
		}
		// If not successful, rest & try again with same server
		time.Sleep(time.Millisecond)
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
// gid is this group's GID, for interacting with the copsmaster.
//
// pass masters[] to copsmaster.MakeClerk() so you can send
// RPCs to the copsmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// cid is the ID of the cluster
// masters gives an array of arrays of ports of shardmasters of clusters
// -> use it to create clerks of all clusters
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	 maxraftstate, cid, gid int, masters [][]*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// DPrintf("creating shard %d server %d\n", gid, me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVSnapshot{})
	labgob.Register(Entry{})
	labgob.Register(PutAfterReply{})
	labgob.Register(GetByVersionReply{})
	labgob.Register(GetShardReply{})
	labgob.Register(DepCheckReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.cid = cid
	kv.masters = masters[cid]
	kv.nclusters = len(masters)

	kv.lastAppliedIndex = 0
	
	for i := 0; i < copsmaster.NShards; i++ {
		kv.kvstore[i] = make(map[string]Entry)
		kv.accepted[i] = false
	}
	
	kv.lastApplied = make(map[int64]CmdResults)

	// Make a copsmaster clerk for each cluster
	kv.mcks = make([]*copsmaster.Clerk, kv.nclusters)
	for i, cluster := range masters {
		kv.mcks[i] = copsmaster.MakeClerk(cluster)
	}
	kv.mck = kv.mcks[kv.cid]
	kv.config = kv.mck.Query(0)
	kv.initiatedConfigChange = false

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.latestTimestamp = 1
	kv.nodeID = uint32(nrand())

	kv.toReplicate = []Op{}

	go kv.backgroundWorker()
	go kv.configPoller()
	go kv.replicationWorker()
	
	DPrintf("shard %d server %d started. current config number %d\n", kv.gid, kv.me, kv.config.Num)

	return kv
}
