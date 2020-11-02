package shardmaster


import (
	"log"
	"os"
	"sync"
	"time"

	"labgob"
	"labrpc"
	"raft"
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
	OpJoin uint8 = iota
	OpLeave
	OpMove
	OpQuery
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	lastApplied map[int64]CmdResults
	lastAppliedIndex int
}

type CmdResults struct {
	Cmd Op
	Config Config
	Err Err
}

type ServerMap map[int][]string

type Op struct {
	ClientID int64
	CommandID uint8

	Type uint8 // Type of operation
	Servers ServerMap // Servers for JoinArgs
	ID int	// Shard for MoveArgs, Num for Query
	ID2 int // GID for MoveArgs
	IDs []int // GIDs in LeaveArgs
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	setWrongLeader := func() {
		reply.WrongLeader = true
	}
	lastAppliedMatch := func()bool {
		sm.mu.Lock()
		lastApplied, ok := sm.lastApplied[args.ClientID]
		sm.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			return true
		} else { return false }
	}
	createCommand := func() Op {
		command := Op { args.ClientID, args.CommandID, OpJoin, args.Servers, 0, 0, []int{} }
		return command
	}
	sm.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
	if reply.WrongLeader == false {
		DPrintf("sm %d Join\n", sm.me, args.Servers)
		DPrintf("sm %d Join return:\n", sm.me, *reply)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	setWrongLeader := func() {
		reply.WrongLeader = true
	}
	lastAppliedMatch := func()bool {
		sm.mu.Lock()
		lastApplied, ok := sm.lastApplied[args.ClientID]
		sm.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			return true
		} else { return false }
	}
	createCommand := func() Op {
		command := Op { args.ClientID, args.CommandID, OpLeave, ServerMap{}, 0, 0, args.GIDs }
		return command
	}
	sm.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
	if reply.WrongLeader == false {
		DPrintf("sm %d Leave\n", sm.me, args.GIDs)
		DPrintf("sm %d Leave return:\n", sm.me, *reply)
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	setWrongLeader := func() {
		reply.WrongLeader = true
	}
	lastAppliedMatch := func()bool {
		sm.mu.Lock()
		lastApplied, ok := sm.lastApplied[args.ClientID]
		sm.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			return true
		} else { return false }
	}
	createCommand := func() Op {
		command := Op { args.ClientID, args.CommandID, OpMove, ServerMap{}, args.Shard, args.GID, []int{} }
		return command
	}
	sm.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
	if reply.WrongLeader == false {
		DPrintf("sm %d Move %d %d\n", sm.me, args.Shard, args.GID)
		DPrintf("sm %d Move return:\n", sm.me, *reply)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	setWrongLeader := func() {
		reply.WrongLeader = true
	}
	lastAppliedMatch := func()bool {
		sm.mu.Lock()
		lastApplied, ok := sm.lastApplied[args.ClientID]
		sm.mu.Unlock()
		if ok && lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			reply.Config = lastApplied.Config
			return true
		} else { return false }
	}
	createCommand := func() Op {
		command := Op { args.ClientID, args.CommandID, OpQuery, ServerMap{}, args.Num, 0, []int{} }
		return command
	}
	sm.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
	if reply.WrongLeader == false {
		DPrintf("sm %d Query %d\n", sm.me, args.Num)
		DPrintf("sm %d Query return:\n", sm.me, *reply)
	}
}

// Code shared by Get and PutAppend RPC handlers
func (sm *ShardMaster) RPCHandler(setWrongLeader func(),
							   lastAppliedMatch func() bool,
							   createCommand func() Op) {
	// If not leader, return WrongLeader
	if _, isLeader := sm.rf.GetState(); !isLeader {
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
	_, term, isLeader := sm.rf.Start(command)
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
		if curTerm, _ := sm.rf.GetState(); curTerm != term {
			setWrongLeader()
			break
		}
		// Yield lock to let background routine apply commands
		time.Sleep(time.Duration(1000000))
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsm tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.lastAppliedIndex = 0
	sm.lastApplied = make(map[int64]CmdResults)

	go sm.backgroundWorker()

	return sm
}

func (sm *ShardMaster) backgroundWorker() {
	for {
		if !sm.rf.IsAlive() {
			return
		}
		committed, ok := sm.readApplyCh()
		if !ok {
			continue
		}
		sm.mu.Lock()
		if committed.CommandValid {
			sm.apply(committed.Command.(Op))
			sm.lastAppliedIndex = committed.CommandIndex
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) readApplyCh() (raft.ApplyMsg, bool) {
	select {
    case applyMsg := <-sm.applyCh:
		return applyMsg, true
	case <-time.After(time.Duration(3000000000)):
        return raft.ApplyMsg{}, false
    }
}

func (sm *ShardMaster) apply(op Op) {
	switch op.Type {
	case OpJoin:
		// Retrieve argument, servers
		servers := op.Servers
		// TODO
		// Get the most recent existing config
		lastConfig := sm.configs[len(sm.configs) - 1]
		// Calculate ID of the next config to be made
		newConfigNum := len(sm.configs)
		// Create new shards & new groups
		var newShards [NShards]int
		newGroups := make(ServerMap)
		// Copy over all servers from old Groups
		for gid, servers := range lastConfig.Groups {
			newGroups[gid] = servers
		}
		// Add servers to newGroups
		for gid, servers := range servers {
			newGroups[gid] = servers
		}
		// Copy over Shards to newShards
		for shard, gid := range lastConfig.Shards {
			newShards[shard] = gid
		}
		// Reassign shards
		newShards = reassignShards(newShards, newGroups)
		// Assemble & append new config
		newConfig := Config{ newConfigNum, newShards, newGroups }
		sm.configs = append(sm.configs, newConfig)
		// Update lastApplied
		sm.lastApplied[op.ClientID] = CmdResults{ op, Config{}, OK }
	case OpLeave:
		// Retrieve argument, GIDs
		GIDs := op.IDs
		// Get the most recent existing config
		lastConfig := sm.configs[len(sm.configs) - 1]
		// Calculate ID of the next config to be made
		newConfigNum := len(sm.configs)
		// Create new shards & new groups
		var newShards [NShards]int
		newGroups := make(ServerMap)
		// Copy over all groups that aren't in GIDs in Groups
		for gid, servers := range lastConfig.Groups {
			inGIDs := false
			for _, g := range GIDs {
				if g == gid {
					inGIDs = true
				}
			}
			if !inGIDs {
				newGroups[gid] = servers
			}
		}
		// Identify shards that no longer have a group assigned
		for shard, gid := range lastConfig.Shards {
			// Copy over gid assigned to each shard from lastConfig
			newShards[shard] = gid
			// If gid is in GIDs, assign 0 instead
			for _, g := range GIDs {
				if gid == g {
					newShards[shard] = 0
				}
			}
		}
		// Reassign shards
		newShards = reassignShards(newShards, newGroups)
		// Assemble & append new config
		newConfig := Config{ newConfigNum, newShards, newGroups }
		sm.configs = append(sm.configs, newConfig)
		// Update lastApplied
		sm.lastApplied[op.ClientID] = CmdResults{ op, Config{}, OK }
	case OpMove:
		// Retrieve arguments, shard & GID
		shard := op.ID
		GID := op.ID2
		// Get the most recent existing config
		lastConfig := sm.configs[len(sm.configs) - 1]
		// Calculate ID of the next config to be made
		newConfigNum := len(sm.configs)
		// Assemble shards array of new config
		var newShards [NShards]int
		for i, _ := range newShards {
			newShards[i] = lastConfig.Shards[i]
		}
		newShards[shard] = GID
		// Assemble & append new config
		newConfig := Config{ newConfigNum, newShards, lastConfig.Groups }
		sm.configs = append(sm.configs, newConfig)
		// Update lastApplied
		sm.lastApplied[op.ClientID] = CmdResults{ op, Config{}, OK }
	case OpQuery:
		num := op.ID
		if num == -1 || num >= len(sm.configs) {
			sm.lastApplied[op.ClientID] = CmdResults{ op, sm.configs[len(sm.configs)-1], OK }
		} else if num > -1 {
			sm.lastApplied[op.ClientID] = CmdResults{ op, sm.configs[num], OK }
		} else {
			sm.lastApplied[op.ClientID] = CmdResults{ op, Config{}, ErrInvalidIndex }
		}
	default:
	}
}

func reassignShards(shards [NShards]int, groups ServerMap) [NShards]int {
	ngroups := len(groups)
	if ngroups == 0 {
		return shards
	}
	// Find min size of partitions (=how many shards each group takes)
	minPartition := NShards / ngroups
	// Find how many groups need to take one extra shard
	excessPartitions := NShards % ngroups
	// Initialize shardcounts: number of shards assigned to each group
	shardcounts := make(map[int]int)
	for gid, _ := range groups {
		shardcounts[gid] = 0
	}
	// Remove shards from groups with too many shards
	for i, gid := range shards {
		count := shardcounts[gid]
		if count < minPartition {
			shardcounts[gid] += 1
		} else if count == minPartition {
			if excessPartitions > 0 {
				shardcounts[gid] += 1
				excessPartitions -= 1
			} else {
				shards[i] = 0
			}
		} else {
			shards[i] = 0
		}
	}
	// Add shards to groups lacking shards
	for group, count := range shardcounts {
		if count < minPartition ||
		   (excessPartitions > 0 && count < minPartition + 1) {
			for i, gid := range shards {
				if count < minPartition ||
				   (excessPartitions > 0 && count < minPartition + 1) {
					if gid == 0 {
						shards[i] = group
						count += 1
						if count == minPartition + 1 {
							excessPartitions -= 1
						}
					}
				} else {
					break
				}
			}
		}
	}
	DPrintf("", shards)
	return shards
}
