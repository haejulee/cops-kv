package copskv

import (
	"time"

	"copsmaster"
)


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
		reply.Err = ErrNotReady
	} else {
		// Otherwise, this group is ready to hand over the shard
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
	// DPrintf("%d-%d initiating config change to %d\n", kv.gid, kv.me, curConfNum+1)
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
	shards := make(map[int]map[string]Entry)
	lastApplied := make(map[int64]CmdResults)
	// Compare cur & next config to find which shards need to be received
	newShards := []int{}
	for i := 0; i < copsmaster.NShards; i++ {
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
		gid := kv.curConfig.Shards[shard]
		// If there is no previous group with the shard, just ignore
		if gid == 0 {
			*ntorecv -= 1
			(*shards)[shard] = make(map[string]Entry)
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
