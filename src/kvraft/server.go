package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
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
	
	ClientID	uint16
	CommandID	uint8
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	lastApplied []Op
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) RegisterClient(args *RegisterClientArgs, reply *RegisterClientReply) {
	kv.mu.Lock()
	// Try to append a RegisterClient command to the log
	var command Op
	command.Type = OpRegisterClient
	index, term, isLeader := kv.rf.Start(command)
	// If not leader, return WrongLeader
	if !isLeader {
		reply.ClientID = -1
	} else { // Else:
		// Read from applyCh until RegisterClient operation shows up
		for {
			// Keep checking if still leader for the same term
			if curTerm, _ := kv.rf.GetState(); curTerm != term {
				reply.ClientID = -1
				break
			}
			// Do a non-blocking read from applyCh
			committed, ok := kv.readApplyCh()
			if !ok {
				continue
			}
			// Apply the command
			if committed.CommandValid {
				kv.apply(committed.Command.(Op))
			}
			// Keep going until the log entry with index is committed
			if committed.CommandIndex == index {
				// If the RegisterClient operation successfully committed
				if committed.CommandValid && 
				   committed.Command.(Op).Type == OpRegisterClient {
					// Return the index of the new element of kv.lastApplied
					reply.ClientID = len(kv.lastApplied) - 1
				} else { // Else
					// Return failure
					reply.ClientID = -1
				}
				break
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) readApplyCh() (raft.ApplyMsg, bool) {
	select {
    case applyMsg := <-kv.applyCh:
        return applyMsg, true
    default:
        return raft.ApplyMsg{}, false
    }
}

func (kv *KVServer) apply(op Op) {
	switch op.Type {
	case OpRegisterClient:
		kv.lastApplied = append(kv.lastApplied, op)
	default:
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
