/* async.go
 Functions & structs related to asynchronous replication between clusters
 - Asynchronous replication goroutine (replicationWorker) + subroutines/forks
 - NeverDepend RPC (invoked from sendNeverDepend, a child of replicationWorker)
 - Applying NeverDepend operations (applyNeverDepend)
 */
package copskv

import (
	"reflect"
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

func (kv *ShardKV) applyNeverDepend(op Op) {
	shard := key2shard(op.Key)
	entry := kv.kvstore[shard][op.Key]
	if entry.Version == op.Version {
	 	entry.NeverDepend = true
	 	kv.kvstore[shard][op.Key] = entry
	}
	kv.lastApplied[op.ClientID] = CmdResults{ op, 0, op.Key, "", OK, op.Version, map[string]uint64{}, true }
}


func (kv *ShardKV) applyAsyncReplicated(op Op) {
	// Check if this operation hasn't been popped yet
	if len(kv.toReplicate) == 0 {
		return
	}
	first := kv.toReplicate[0]
	if first.ClientID != op.ClientID || first.CommandID != op.CommandID {
		return
	}
	DPrintf("%d-%d-%d applying AsyncReplicated %d-%d\n", kv.cid, kv.gid, kv.me, op.ClientID, op.CommandID)
	// If not popped yet, pop from kv.toReplicate
	if len(kv.toReplicate) == 1 {
		kv.toReplicate = []Op{}
	} else {
		kv.toReplicate = append([]Op{}, kv.toReplicate[1:]...)
	}
}

// When leader, asynchronously replicates put operations
func (kv *ShardKV) replicationWorker() {
	for {
		// If server has been killed, terminate this routine as well
		if !kv.rf.IsAlive() {
			return
		}
		// Check if there are any puts to replicate across wide-area
		// (only do if leader)
		_, isLeader := kv.rf.GetState()
		kv.mu.Lock()
		if isLeader && len(kv.toReplicate) > 0 {
			// Pick the first operation in the queue to replicate
			op := kv.toReplicate[0]
			kv.mu.Unlock()
			// Create counter & mutex to protect counter
			mu := sync.Mutex{}
			count := 0
			mu.Lock()
			// Replicate operation to other clusters
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
			// Commit completed async replication to log
			command := Op{
				Type: OpAsyncReplicated,
				ClientID: op.ClientID,
				CommandID: op.CommandID,
			}
			_, term, leader := kv.rf.Start(command)
			if !leader {
				continue
			}
			// Wait until committed & applied
			for {
				kv.mu.Lock()
				// Break when 1st element of toReplicate no longer matches the replicated operation
				if len(kv.toReplicate) == 0 || !reflect.DeepEqual(kv.toReplicate[0], op) {
					kv.mu.Unlock()
					break
				}
				kv.mu.Unlock()
				// Also break if no longer leader for same term
				if t, l := kv.rf.GetState(); !l || t != term {
					break
				}
				time.Sleep(10 * time.Millisecond)
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
