/* async.go
 Functions & structs related to asynchronous replication between clusters
 - Asynchronous replication goroutine (replicationWorker) + subroutines/forks
 - NeverDepend RPC (invoked from sendNeverDepend, a child of replicationWorker)
 */
package copskv

import (
	"sync"
	"time"
)


type NeverDependArgs struct {
	Key string
	Version uint64
	ClientID int64
}

type NeverDependReply struct {
	WrongLeader bool
	Err Err
}

func (kv *ShardKV) NeverDepend(args *NeverDependArgs, reply *NeverDependReply) {
	command := Op{}
	command.Type = OpNeverDepend
	command.Key = args.Key
	command.Version = args.Version
	command.ClientID = args.ClientID
	_, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	for {
		// If matching command applied, return
		kv.mu.Lock()
		lastApplied, ok := kv.lastApplied[args.ClientID]
		kv.mu.Unlock()
		if ok && lastApplied.Cmd.Type == OpNeverDepend &&
		   lastApplied.Cmd.Version == args.Version {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			break
		}
		// Keep checking if still leader (for the same term)
		if curTerm, _ := kv.rf.GetState(); curTerm != term {
			reply.WrongLeader = true
			break
		}
		// Yield lock to let background routine apply commands
		time.Sleep(10 * time.Millisecond)
	}
}


// Asynchronously replicates put operations
// TODO: integrate kv.toReplicate into consensus
//       All servers build up toReplicate from applied commands
//       Leader replicates 1st cmd, then commits
//       all servers pop cmd from toReplicate
//       Having multiple replication attempts for same cmd shouldn't be a problem (when changing leaders)
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
			// DPrintf("%d-%d-%d replicating op", kv.cid, kv.gid, kv.me, op)
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
			// DPrintf("XXX nclusters %d", kv.nclusters, kv.mcks)
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
			// send out never_depend for key:version
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
					go kv.sendNeverDepend(op, group)
				}
			}
		} else {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) replicateToCluster(op Op, group []string, count *int, mu *sync.Mutex) {
	// DPrintf("%d-%d-%d replicating op to other cluster", kv.cid, kv.gid, kv.me, op)
	args := PutAfterArgs{ op.Key, op.Value, op.Nearest, op.Version, op.ClientID, op.CommandID, op.ConfigNum }
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

func (kv *ShardKV) sendNeverDepend(op Op, group []string) {
	args := NeverDependArgs{ op.Key, op.Version, op.ClientID }
	for i := 0; ; {
		srv := kv.make_end(group[i])
		var reply NeverDependReply
		ok := srv.Call("ShardKV.NeverDepend", &args, &reply)
		if !ok || reply.WrongLeader {
			i = (i + 1) % len(group)
			time.Sleep(time.Millisecond)
			continue
		}
		if reply.Err == OK {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
