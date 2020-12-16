package copskv

import (
	"sync"
	"time"
)


func (kv *ShardKV) DepCheck(args *DepCheckArgs, reply *DepCheckReply) {
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
