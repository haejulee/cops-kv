/* server.go
 Contains the 'main' server code:
 - Initializing & killing the server
 - All client-facing RPCs (GetByVersion, PutAfter)
 - Applying kv-store state changes
 - Snapshotting
 */
package copskv

import (
	"time"

	"labgob"
	"labrpc"
	"raft"
	"copsmaster"
)


func (kv *ShardKV) GetByVersion(args *GetByVersionArgs, reply *GetByVersionReply) {
	// DPrintf("server %d-%d handling get\n", kv.gid, kv.me)
	
	setWrongLeader := func() {
		// DPrintf("ShardKV %d WrongLeader Get %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}

	accepted := func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if kv.curConfig.Num != args.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != args.ConfigNum) {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			// DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			return false
		}
		shard := key2shard(args.Key)
		if !kv.accepted[shard] {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			// DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
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
			reply.Version = lastApplied.Version
			reply.Deps = lastApplied.Deps
			reply.NeverDepend = lastApplied.NeverDepend
			if reply.Err == OK {
				DPrintf("%d-%d-%d successfully returning Get %s: %s\n", kv.cid, kv.gid, kv.me, args.Key, reply.Value)
			} else {
				// DPrintf("%d-%d error returning Get %d-%d\n", kv.gid, kv.me, args.ClientID, args.CommandID)
			}
			if reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {
				delete(kv.lastApplied, args.ClientID)
			}
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		return Op { 
			Type: OpGetByVersion,
			Key: args.Key,
			Version: args.Version,
			ClientID: args.ClientID,
			CommandID: args.CommandID,
			ConfigNum: args.ConfigNum,
		}
	}
	kv.RPCHandler(setWrongLeader, accepted, lastAppliedMatch, createCommand)

}

func (kv *ShardKV) PutAfter(args *PutAfterArgs, reply *PutAfterReply) {
	// If version is nil, synchronous put_after -> immediately commit put
	if args.Version == 0 {
		kv.PutAfterHandler(args, reply)
		return
	}
	// Else, handle async put_after -> commit after dependencies met
	// DPrintf("%d-%d-%d Received async PutAfter\n", kv.cid, kv.gid, kv.me)

	// If not leader, return
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	// If not up to date to ConfigNum, return wronggroup
	if kv.curConfig.Num != args.ConfigNum &&
		(!kv.interConf || kv.nextConfig.Num != args.ConfigNum) {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		// DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	// Make sure key is in shard
	shard := key2shard(args.Key)
	if !kv.accepted[shard] {
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		// DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
		kv.mu.Unlock()
		return
	}
	
	// Make sure dependencies are met
	// DPrintf("%d-%d-%d Doing dependency checks\n", kv.cid, kv.gid, kv.me)
	kv.mu.Unlock()
	kv.doDepChecks(args.Nearest)
	
	// Once dependencies are met, commit put
	// DPrintf("%d-%d-%d Committing async PutAfter\n", kv.cid, kv.gid, kv.me)
	kv.asyncputmu.Lock()
	kv.PutAfterHandler(args, reply)
	kv.asyncputmu.Unlock()
	// DPrintf("%d-%d-%d Returning async PutAfter\n", kv.cid, kv.gid, kv.me)
}

func (kv *ShardKV) PutAfterHandler(args *PutAfterArgs, reply *PutAfterReply) {
	// DPrintf("server %d-%d handling putappend\n", kv.gid, kv.me)

	setWrongLeader := func() {
		// DPrintf("ShardKV %d WrongLeader PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}

	accepted := func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if kv.curConfig.Num != args.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != args.ConfigNum) {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			// DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
			return false
		}
		shard := key2shard(args.Key)
		if !kv.accepted[shard] {
			reply.WrongLeader = false
			reply.Err = ErrWrongGroup
			// DPrintf("%d-%d error returning Get %s %d-%d - Wrong group\n", kv.gid, kv.me, args.Key, args.ClientID, args.CommandID)
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
			reply.Version = lastApplied.Version
			if reply.Err == OK {
				DPrintf("%d-%d-%d successfully returning Put %s->%s\n", kv.cid, kv.gid, kv.me, args.Key, args.Value)
			}
			if reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {
				delete(kv.lastApplied, args.ClientID)
			}
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		command := Op {
			Type: OpPutAfter,
			Key: args.Key,
			Value: args.Value,
			Version: args.Version,
			Nearest: args.Nearest,
			ClientID: args.ClientID,
			CommandID: args.CommandID,
			ConfigNum: kv.curConfig.Num,
		}
		return command
	}

	kv.RPCHandler(setWrongLeader, accepted, lastAppliedMatch, createCommand)
}

// Code shared by GetByVersion and PutAfter RPC handlers
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
		// DPrintf("%d-%d-%d PPP", kv.cid, kv.gid, kv.me)
		time.Sleep(10 * time.Millisecond)
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
	// DPrintf("%d-%d-%d applying operation", kv.cid, kv.gid, kv.me)
	if op.Type == OpGetByVersion || op.Type == OpPutAfter || op.Type == OpNeverDepend {
		// If not at or transitioning to the config num, return error
		if kv.curConfig.Num != op.ConfigNum &&
		   (!kv.interConf || kv.nextConfig.Num != op.ConfigNum) {
		   kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup, op.Version, map[string]uint64{}, false }
			return
		}
		// If shard not accepted, return error
		shard := key2shard(op.Key)
		if !kv.accepted[shard] {
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrWrongGroup, op.Version, map[string]uint64{}, false }
			return
		}
	}
	switch op.Type {
	case OpGetByVersion:
		kv.applyGetByVersion(op)
	case OpPutAfter:
		kv.applyPutAfter(op)
	case OpAsyncReplicated:
		kv.applyAsyncReplicated(op)
	case OpNeverDepend:
		kv.applyNeverDepend(op)
	case OpConfChangePrep:
		kv.applyConfChangePrep(op)
	case OpConfChange:
		kv.applyConfChange(op)
	default:
		DPrintf("unrecognized operation\n")
	}
}

func (kv *ShardKV) applyGetByVersion(op Op) {
	shard := key2shard(op.Key)
	entry, ok := kv.kvstore[shard][op.Key]
	if ok {
		kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, entry.Value, OK, entry.Version, entry.Deps, entry.NeverDepend }
	} else {
		kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", ErrNoKey, 0, map[string]uint64{}, false }
	}
}

func (kv *ShardKV) applyPutAfter(op Op) {
	shard := key2shard(op.Key)
	if kv.lastApplied[op.ClientID].Cmd.CommandID != op.CommandID {
		// Determining version of put operation
		version := op.Version
		if version == 0 {
			// If version argument was nil, generate new version number & apply immediately
			version = kv.versionNumber()
			kv.kvstore[shard][op.Key] = Entry{ version, op.Value, op.Nearest, false }
			kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK, version, op.Nearest, false }
			// Append put operation with the generated version number to replication queue
			// DPrintf("%d-%d-%d adding to toReplicate", kv.cid, kv.gid, kv.me)
			op.Version = version
			// Add PutAfter operation to replication queue
			kv.toReplicate = append(kv.toReplicate, op)
		} else {
			// If version specified,
			// update latest timestamp using given version
			kv.updateTimestamp(vtot(version))
			// Apply put operation, only if new version is higher than existing
			entry, ok := kv.kvstore[shard][op.Key]
			// DPrintf("async put %s:%s", op.Key, op.Value)
			if !ok || vtot(entry.Version) < vtot(version) ||
				(vtot(entry.Version) == vtot(version) && uint32(entry.Version) < uint32(version)) {
				// DPrintf("writing async put %s:%s", op.Key, op.Value)
				kv.kvstore[shard][op.Key] = Entry{ version, op.Value, op.Nearest, false }
				kv.lastApplied[op.ClientID] = CmdResults{ op, op.CommandID, op.Key, "", OK, version, op.Nearest, false }
			}
			// DPrintf("CCC")
		}
	}
}

// Apply snapshot received through applyCh
func (kv *ShardKV) applySnapshot(snapshot KVSnapshot) {
	// DPrintf("applying snapshot\n")
	kv.lastApplied = snapshot.LastApplied
	kv.kvstore = snapshot.KVStore
	kv.lastAppliedIndex = snapshot.LastAppliedIndex
	kv.interConf = snapshot.InterConf
	kv.curConfig = snapshot.CurConfig
	kv.nextConfig = snapshot.NextConfig
	kv.accepted = snapshot.Accepted
	kv.toReplicate = snapshot.ToReplicate
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
				kv.toReplicate,
			}
			kv.rf.Snapshot(snapshot, kv.lastAppliedIndex)
		}
		// DPrintf("%d-%d done processing commit\n", kv.gid, kv.me)
		kv.mu.Unlock()
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
	kv.curConfig = kv.mck.Query(0)
	kv.nextConfig = copsmaster.Config{}
	kv.interConf = false

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.latestTimestamp = 1
	kv.nodeID = uint32(nrand())

	kv.toReplicate = []Op{}

	go kv.backgroundWorker()
	go kv.configWorker()
	go kv.replicationWorker()
	
	DPrintf("shard %d server %d started. current config number %d\n", kv.gid, kv.me, kv.curConfig.Num)

	return kv
}
