package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"os"
	"raft"
	"sync"
	"time"
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
)

// The struct representing each Put/Append/Get command in the Raft log
type Op struct {
	Type 		uint8
	Key			string
	Value		string
	
	ClientID	int
	CommandID	uint8
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate 	 int // snapshot if log grows this big
	lastAppliedIndex int // log index of last applied command

	lastApplied []CmdResults
	kvstore		map[string]string
}

type CmdResults struct {
	Cmd Op
	Value string
	Err Err
}

type KVSnapshot struct {
	LastApplied []CmdResults
	KVStore map[string]string
	LastAppliedIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	setWrongLeader := func() {
		DPrintf("KVServer %d WrongLeader Get %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}
	
	lastAppliedMatch := func(msg raft.ApplyMsg, index int) bool {
		if msg.CommandIndex == index {
			lastApplied := kv.lastApplied[args.ClientID]
			if lastApplied.Cmd.CommandID == args.CommandID {
				reply.WrongLeader = false
				reply.Err = lastApplied.Err
				reply.Value = lastApplied.Value
				DPrintf("KVServer %d successfully returning Get %d-%d\n", kv.me, args.ClientID, args.CommandID)
			} else {
				reply.WrongLeader = true
			}
			return true
		} else { return false }
	}
	
	createCommand := func() Op {
		return Op { OpGet, args.Key, "", args.ClientID, args.CommandID }
	}
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
	DPrintf("KVServer %d Get %d-%d: WrongLeader %t %p\n", kv.me, args.ClientID, args.CommandID, reply.WrongLeader, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	setWrongLeader := func() {
		DPrintf("KVServer %d WrongLeader PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
		reply.WrongLeader = true
	}
	
	lastAppliedMatch := func(msg raft.ApplyMsg, index int) bool {
		// If the client's registration isn't applied to the server yet,
		// the client's request can't have been applied yet.
		if len(kv.lastApplied) <= args.ClientID { return false }
		// If client has been registered, check the last applied cmd for
		// the client & see if its command ID matches the one of our request
		lastApplied := kv.lastApplied[args.ClientID]
		if lastApplied.Cmd.CommandID == args.CommandID {
			reply.WrongLeader = false
			reply.Err = lastApplied.Err
			DPrintf("KVServer %d successfully returning PutAppend %d-%d\n", kv.me, args.ClientID, args.CommandID)
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
	DPrintf("KVServer %d PutAppend %d-%d: WrongLeader %t %p\n", kv.me, args.ClientID, args.CommandID, reply.WrongLeader, reply)
}

func (kv *KVServer) RegisterClient(args *RegisterClientArgs, reply *RegisterClientReply) {
	setWrongLeader := func() { reply.ClientID = -1 }
	
	lastAppliedMatch := func(msg raft.ApplyMsg, index int) bool {
		if msg.CommandIndex == index {
			if msg.CommandValid &&
			   msg.Command.(Op).Type == OpRegisterClient {
				reply.ClientID = len(kv.lastApplied) - 1
				DPrintf("KVServer %d assigned ClientID %d\n", kv.me, reply.ClientID)
			} else {
				DPrintf("KVServer %d failed to assign a ClientID\n", kv.me)
				reply.ClientID = -1
			}
			return true
		} else {
			return false
		}
	}
	
	createCommand := func() (command Op) {
		command.Type = OpRegisterClient
		return
	}
	
	kv.RPCHandler(setWrongLeader, lastAppliedMatch, createCommand)
}


// Code shared by Get, PutAppend, and RegisterClient RPC handlers
func (kv *KVServer) RPCHandler(setWrongLeader func(),
							   lastAppliedMatch func(raft.ApplyMsg, int) bool,
							   createCommand func() Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// If not leader, return WrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		setWrongLeader()
		return
	}
	// If lastApplied has the command, return response
	if (lastAppliedMatch(raft.ApplyMsg{}, -1)) {
		return
	}
	// Construct Op struct for the received request
	command := createCommand()
	// Start consensus for the request
	index, term, isLeader := kv.rf.Start(command)
	if !isLeader {
		setWrongLeader()
		return
	}
	// Read applyCh until 1st occurrence of the request
	for {
		// Keep checking if still leader (for the same term)
		if curTerm, _ := kv.rf.GetState(); curTerm != term {
			setWrongLeader()
			break
		}
		// Do a non-blocking read from applyCh
		committed, ok := kv.readApplyCh()
		if !ok {
			continue
		}
		// If a log is read from applyCh:
		if committed.CommandValid {
			cmd := committed.Command.(Op)
			// Apply the command to kvstore
			kv.apply(cmd)
			kv.lastAppliedIndex = committed.CommandIndex
			// Check if the applied cmd matches our request
			if (lastAppliedMatch(committed, index)) {
				break
			}
		} else {
			kv.applySnapshot(committed.Command.(KVSnapshot))
		}
	}
}


// Perform a non-blocking read from applyChannel
func (kv *KVServer) readApplyCh() (raft.ApplyMsg, bool) {
	select {
    case applyMsg := <-kv.applyCh:
        return applyMsg, true
    default:
        return raft.ApplyMsg{}, false
    }
}

// Apply a committed command to local state
func (kv *KVServer) apply(op Op) {
	switch op.Type {
	case OpRegisterClient:
		kv.lastApplied = append(kv.lastApplied, CmdResults{op, "", OK})
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

func (kv *KVServer) applySnapshot(snapshot KVSnapshot) {
	kv.lastApplied = snapshot.LastApplied
	kv.kvstore = snapshot.KVStore
	kv.lastAppliedIndex = snapshot.LastAppliedIndex
}


// Read newly committed commands from applyCh and apply them periodically
// Runs in a separate routine in the background
func (kv *KVServer) bgReadApplyCh() {
	var committed raft.ApplyMsg
	for {
		kv.mu.Lock()
		if !kv.rf.IsAlive() {
			kv.mu.Unlock()
			return
		}
		for ok := true; ok; {
			// Do a non-blocking read from applyCh
			committed, ok = kv.readApplyCh()
			// Apply the newly committed command
			if ok {
				if committed.CommandValid {
					kv.apply(committed.Command.(Op))
					kv.lastAppliedIndex = committed.CommandIndex
				} else {
					kv.applySnapshot(committed.Command.(KVSnapshot))
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(100000000))
	}
}

func (kv *KVServer) bgSnapshotter() {
	for {
		kv.mu.Lock()
		if !kv.rf.IsAlive() {
			kv.mu.Unlock()
			return
		}
		if kv.rf.StateSizeLimitReached(kv.maxraftstate) {
			snapshot := KVSnapshot{ kv.lastApplied, kv.kvstore, kv.lastAppliedIndex }
			kv.rf.Snapshot(snapshot, kv.lastAppliedIndex)
		}
		kv.mu.Unlock()
		time.Sleep(time.Duration(100000000))
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	DPrintf("Killing server %d\n", kv.me)
	kv.rf.Kill()
	DPrintf("Killed server %d\n", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	DPrintf("Starting KVServer %d\n", me)
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVSnapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.lastAppliedIndex = 0
	kv.kvstore = make(map[string]string)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf("KVServer %d started\n", kv.me)

	// You may need initialization code here.
	go kv.bgReadApplyCh()
	go kv.bgSnapshotter()

	return kv
}
