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
	OpConfChangePrep // stop accepting requests for keys not in next config
	OpConfChange     // apply keys gained in next config to state - discard old config
)


type Op struct {
	Type uint8
	Key string
	Value string

	ClientID int64
	CommandID uint8
	ConfigNum int

	Config shardmaster.Config // for ConfChangePrep

	ShardStore map[int]map[string]string // for ConfChange
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

	interConf    bool // true if group is in between two configs
	curConfig    shardmaster.Config // current config (old config if interConf true)
	nextConfig   shardmaster.Config // new config, used when interConf true
	accepted     [shardmaster.NShards]bool // to track which shards are currently accepted in the group

	maxraftstate int // snapshot if log grows this big
	lastAppliedIndex int // log index of last applied command

	lastApplied map[int64]CmdResults
	kvstore [shardmaster.NShards]map[string]string // keep a separate k-v store for each shard
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

	InterConf bool
	CurConfig  shardmaster.Config
	NextConfig shardmaster.Config
	Accepted   [shardmaster.NShards]bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// DPrintf("server %d-%d handling get\n", kv.gid, kv.me)
	
	setWrongLeader := func() {
		// DPrintf("ShardKV %d WrongLeader Get %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}

	accepted := func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		// Set ErrWrongGroup & return false if not yet at or transitioning to the
		// config num specified in the request.
		if kv.curConfig.Num != args.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != args.ConfigNum) {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			return false
		}
		// Set ErrWrongGroup & return false if shard for key is not currently accepted
		// within this group. This will be the case when
		// A. the group isn't in charge of the shard in the current config
		// B. the leader initiated a config change and the group isn't in charge of the shard in next config
		shard := key2shard(args.Key)
		if !kv.accepted[shard] {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			return false
		}
		return true
	}
	
	lastAppliedMatch := func() bool {
		kv.mu.Lock()
		lastApplied, ok := kv.lastApplied[args.ClientID]
		kv.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			reply.Value = lastApplied.Value
			if reply.Err == OK {
				DPrintf("%d-%d successfully returning Get %d-%d\n", kv.gid, kv.me, args.ClientID, args.CommandID)
			} else if reply.Err == ErrNotLeader {
				DPrintf("%d-%d error returning Get %s %d-%d - Not leader\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			} else if reply.Err == ErrWrongGroup {
				DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			}
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		return Op {
			Type: OpGet,
			Key: args.Key,
			ClientID: args.ClientID,
			CommandID: args.CommandID,
			ConfigNum: args.ConfigNum,
		}
	}
	kv.RPCHandler(setWrongLeader, accepted, lastAppliedMatch, createCommand)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// DPrintf("server %d-%d handling putappend\n", kv.gid, kv.me)
	
	setWrongLeader := func() {
		// DPrintf("ShardKV %d WrongLeader PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}

	accepted := func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		// Set ErrWrongGroup & return false if not yet at or transitioning to the
		// config num specified in the request.
		if kv.curConfig.Num != args.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != args.ConfigNum) {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			return false
		}
		// Set ErrWrongGroup & return false if shard for key is not currently accepted
		// within this group. This will be the case when
		// A. the group isn't in charge of the shard in the current config
		// B. the leader initiated a config change and the group isn't in charge of the shard in next config
		shard := key2shard(args.Key)
		if !kv.accepted[shard] {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			return false
		}
		return true
	}
	
	lastAppliedMatch := func() bool {
		kv.mu.Lock()
		lastApplied, ok := kv.lastApplied[args.ClientID]
		kv.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			if reply.Err == OK {
				DPrintf("%d-%d successfully returning PutAppend %d-%d\n", kv.gid, kv.me, args.ClientID, args.CommandID)
			} else if reply.Err == ErrNotLeader {
				DPrintf("%d-%d error returning PutAppend %s %d-%d - Not leader\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			} else if reply.Err == ErrWrongGroup {
				DPrintf("%d-%d error returning PutAppend %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			}
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		command := Op {
			Key: args.Key,
			Value: args.Value,
			ClientID: args.ClientID,
			CommandID: args.CommandID,
			ConfigNum: args.ConfigNum,
		}
		if args.Op == "Put" {
			command.Type = OpPut
		} else {
			command.Type = OpAppend
		}
		return command
	}
	kv.RPCHandler(setWrongLeader, accepted, lastAppliedMatch, createCommand)
}

// Code shared by Get and PutAppend RPC handlers
func (kv *ShardKV) RPCHandler(setWrongLeader func(),
								accepted func() bool,
							   lastAppliedMatch func() bool,
							   createCommand func() Op) {
	// If not leader, return WrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		setWrongLeader()
		return
	}
	// If shard currently not accepted by group, return ErrWrongGroup
	if !accepted() {
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
	defer kv.mu.Unlock()
	// Only respond if leader of the group
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrNotLeader
		return
	}
	if kv.curConfig.Num < args.ConfigNum {
		// If not yet up to date with the config number
		// of the shard being requested, return ErrNotReady
		reply.Err = ErrNotReady
	} else if kv.curConfig.Num == args.ConfigNum && !kv.interConf {
		// If current config number is exactly that being requested,
		// but the change to next config hasn't started yet in this group,
		// also return ErrNotReady
		reply.Err = ErrWrongGroup
	} else {
		// Otherwise, this group is ready to hand over the shard
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
	switch op.Type {
	case OpGet:
		// If not at or transitioning to the config num, return error
		if kv.curConfig.Num != op.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != op.ConfigNum) {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		// If shard not accepted, return error
		shard := key2shard(op.Key)
		if !kv.accepted[shard] {
			DPrintf("%s (shard %d) not accepted\n", op.Key, shard)
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
		// If not at or transitioning to the config num, return error
		if kv.curConfig.Num != op.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != op.ConfigNum) {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		// If shard not accepted, return error
		shard := key2shard(op.Key)
		if !kv.accepted[shard] {
			DPrintf("%s (shard %d) not accepted\n", op.Key, shard)
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			kv.kvstore[shard][op.Key] = op.Value
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK }
		}
	case OpAppend:
		// If not at or transitioning to the config num, return error
		if kv.curConfig.Num != op.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != op.ConfigNum) {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		// If shard not accepted, return error
		shard := key2shard(op.Key)
		if !kv.accepted[shard] {
			DPrintf("%s (shard %d) not accepted\n", op.Key, shard)
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup }
			return
		}
		if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
			val, ok := kv.kvstore[shard][op.Key]
			if ok {
				kv.kvstore[shard][op.Key] = val + op.Value
				kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK }
			} else {
				kv.kvstore[shard][op.Key] = op.Value
				kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrNoKey }
			}
		}
	case OpConfChangePrep:
		// If currently at the config prior to given config
		if kv.curConfig.Num == op.Config.Num - 1 {
			// If not already in inter-config mode
			if !kv.interConf {
				// Go into inter-config mode & modify currently accepted shards
				kv.interConf = true
				// For each shard, set kv.accepted[shard] to false if no longer covered
				// by current group in the new config.
				for shard, accepted := range kv.accepted {
					if accepted {
						newgid := op.Config.Shards[shard]
						if newgid != kv.gid {
							kv.accepted[shard] = false
						}
					}
				}
				// Update next config
				kv.nextConfig = op.Config
			}
		}
	case OpConfChange:
		// If already past this config change, ignore
		if kv.curConfig.Num >= op.Config.Num {
			return
		}
		// DPrintf("%d-%d changing to config %d\n", kv.gid, kv.me, op.Config.Num)
		// Write additions to kv.kvstore & kv.accepted
		for shard, store := range op.ShardStore {
			kv.kvstore[shard] = store
			kv.accepted[shard] = true
		}
		DPrintf("%d-%d accepted:", kv.gid, kv.me, kv.accepted)
		// Write additions to kv.lastApplied
		for clientID, cmdres := range op.LastApplied {
			c, ok := kv.lastApplied[clientID]
			if !ok {
				// If there's no entry for clientID in kv.lastApplied, write
				// received entry to kv.lastApplied[clientID]
				kv.lastApplied[clientID] = cmdres
			} else if cmdres.CommandID > c.CommandID {
				// If the received entry is later than existing, overwrite
				// received entry to kv.lastApplied[clientID]
				kv.lastApplied[clientID] = cmdres
			}
		}
		// Reset kv.interConf
		kv.interConf = false
		// Update current & next config
		kv.curConfig = kv.nextConfig
		kv.nextConfig = shardmaster.Config{}
		DPrintf("%d-%d changed to config %d\n", kv.gid, kv.me, op.Config.Num)
	default:
		DPrintf("unrecognized operation\n")
	}
}

func (kv *ShardKV) applySnapshot(snapshot KVSnapshot) {
	DPrintf("applying snapshot\n")
	kv.lastApplied = snapshot.LastApplied
	kv.kvstore = snapshot.KVStore
	kv.lastAppliedIndex = snapshot.LastAppliedIndex
	kv.interConf = snapshot.InterConf
	kv.curConfig = snapshot.CurConfig
	kv.nextConfig = snapshot.NextConfig
	kv.accepted = snapshot.Accepted
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
			snapshot := KVSnapshot{
				kv.lastApplied,
				kv.kvstore,
				kv.lastAppliedIndex,
				kv.interConf,
				kv.curConfig,
				kv.nextConfig,
				kv.accepted,
			}
			kv.rf.Snapshot(snapshot, kv.lastAppliedIndex)
		}
		// DPrintf("%d-%d done processing commit\n", kv.gid, kv.me)
		kv.mu.Unlock()
	}
}

// When leader, periodically handles config changes
// A) checks for config updates to start new config changes
// B) checks for started config changes to complete them
func (kv *ShardKV) configWorker() {
	for {
		// If server has been killed, terminate this routine as well
		if !kv.rf.IsAlive() {
			return
		}
		// If not leader, do nothing
		if _, isLeader := kv.rf.GetState(); !isLeader {
			goto Sleep
		}
		// Acquire lock to check if group is in inter-config mode
		kv.mu.Lock()
		if kv.interConf { // If inter-config, retrieve new shards to complete transfer
			kv.executeConfigChange()
			continue
		} else { // If not inter-config, check for config updates
			// Save current config num before unlocking
			curConfNum := kv.curConfig.Num
			kv.mu.Unlock()
			// If latest config is greater than group's current config
			// initiate new config change
			if kv.mck.Query(-1).Num > curConfNum {
				kv.initiateConfigChange(curConfNum)
			}
		}
		Sleep:
		// Sleep for 90 milliseconds
		time.Sleep(90 * time.Millisecond)
	}
}

// Lock not held when called
func (kv *ShardKV) initiateConfigChange(curConfNum int) {
	DPrintf("%d-%d initiating config change to %d\n", kv.gid, kv.me, curConfNum+1)
	// Start consensus on initiating change to next config
	nextConfig := kv.mck.Query(curConfNum + 1)
	cmd := Op {
		Type: OpConfChangePrep,
		Config: nextConfig,
	}
	_, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return
	}
	// Wait until config change initiated or no longer leader
	for {
		// If switched to inter-config mode, break
		kv.mu.Lock()
		if kv.interConf {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		// If in different term / no longer leader, also break
		if t, l := kv.rf.GetState(); !l || t != term {
			break
		}
		// Sleep to let other routines run
		time.Sleep(10 * time.Millisecond)
	}
}

// Lock held when called, not held when returns
func (kv *ShardKV) executeConfigChange() {
	// Initialize structures to store incoming shard info
	shards := make(map[int]map[string]string)
	lastApplied := make(map[int64]CmdResults)
	// Compare cur & next config to find which shards need to be received
	newShards := []int{}
	for i := 0; i < shardmaster.NShards; i++ {
		// If a shard is not owned by group in cur config
		// but owned by group in next config, add to newShards
		if kv.curConfig.Shards[i] != kv.gid &&
		   kv.nextConfig.Shards[i] == kv.gid {
			newShards = append(newShards, i)
		}
	}
	// Start routines to request shards from other groups
	ntorecv := len(newShards)
	for _, shard := range newShards {
		go kv.retrieveShard(shard, &shards, &lastApplied, &ntorecv)
	}
	// Wait until all shards have been received
	for ntorecv > 0 {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		if !kv.rf.IsAlive() {
			kv.mu.Lock()
			return
		}
		kv.mu.Lock()
	}
	// Commit config change to log to apply it to whole group
	cmd := Op {
		Type: OpConfChange,
		Config: kv.nextConfig,
		ShardStore: shards,
		LastApplied: lastApplied,
	}
	nextConfNum := kv.nextConfig.Num
	kv.mu.Unlock()
	_, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return
	}
	// Wait until config change applied or no longer leader for term
	for {
		time.Sleep(10 * time.Millisecond)
		// If no longer leader for term, break
		if t, l := kv.rf.GetState(); !l || t != term {
			break
		}
		// If config change succeeded, break
		kv.mu.Lock()
		if kv.curConfig.Num == nextConfNum {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
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
		// DPrintf("%d-%d retrieving shard %d\n", kv.gid, kv.me, shard)
		gid := kv.curConfig.Shards[shard]
		// If there is no previous group with the shard, just ignore
		if gid == 0 {
			*ntorecv -= 1
			(*shards)[shard] = make(map[string]string)
			kv.mu.Unlock()
			return
		}
		// DPrintf("%d-%d gid!=0\n", kv.gid, kv.me)
		// Else, get shard from previous group
		group := kv.curConfig.Groups[gid]
		args := GetShardArgs{ kv.curConfig.Num, shard }
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
	// DPrintf("creating shard %d server %d\n", gid, me)
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
	kv.curConfig = kv.mck.Query(0)
	kv.nextConfig = shardmaster.Config{}
	kv.interConf = false

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.backgroundWorker()
	go kv.configWorker()
	
	DPrintf("shard %d server %d started. current config number %d\n", kv.gid, kv.me, kv.curConfig.Num)

	return kv
}
