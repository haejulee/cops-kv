package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	"labrpc"
)


type Clerk struct {
	servers			[]*labrpc.ClientEnd
	lastLeader		int
	clientID		int64
	nextCommandID	uint8
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// Make a new Clerk struct
	ck := new(Clerk)
	// Save servers to Clerk struct
	ck.servers = servers
	// Initialize clientID & nextCommandID
	ck.clientID = nrand()
	ck.nextCommandID = 1
	// Return the Clerk struct
	DPrintf("Made a clerk!\n")
	return ck
}

func (ck *Clerk) randomServer() int {
	return int(nrand() % int64(len(ck.servers)))
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// Initialize arguments & reply struct
	args := GetArgs { key, ck.clientID, ck.nextCommandID }
	// DPrintf("Client %d requesting Get %d\n", args.ClientID, args.CommandID)
	ck.nextCommandID += 1
	ok := false
	// Loop while ok == false
	for i := ck.lastLeader; !ok; i = (i+1) % len(ck.servers) {
		var reply GetReply
		// Send Get RPC to a server
		ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
		// If RPC succeeded:
		if ok {
			// DPrintf("%d-%d reply: WrongLeader=%t, Value=%s %p\n", args.ClientID, args.CommandID, reply.WrongLeader, reply.Value, &reply)
			if reply.WrongLeader == true {
				// DPrintf("Client %d received WrongLeader for %d\n", args.ClientID, args.CommandID)
				// If wrong leader, set ok = false
				ok = false
			} else if reply.Err != OK {
				// DPrintf("Client %d received Error for %d\n", args.ClientID, args.CommandID)
				// If error, return empty string
				return ""
			} else {
				// DPrintf("Client %d received Get response %d\n", args.ClientID, args.CommandID)
				// Else, update last leader
				ck.lastLeader = i
				// Return value
				return reply.Value
			}
		} else {
			DPrintf("Client %d network failure: Get %d\n", args.ClientID, args.CommandID)
		}
		if !ok {
			time.Sleep(time.Duration(10000000))
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// Initialize arguments & reply struct
	args := PutAppendArgs { key, value, op, ck.clientID, ck.nextCommandID }
	// DPrintf("Client %d requesting PutAppend %d\n", args.ClientID, args.CommandID)
	ck.nextCommandID += 1
	ok := false
	// Loop while ok == false
	for i := ck.lastLeader; !ok; i = (i+1) % len(ck.servers) {
		var reply PutAppendReply
		// Send PutAppend RPC to a server
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		// If RPC succeeded:
		if ok {
			if reply.WrongLeader == true {
				// If wrong leader, set ok = false
				ok = false
			} else {
				// Else, update last leader
				ck.lastLeader = i
			}
		} else {
			DPrintf("Client %d network failure: PutAppend %d\n", args.ClientID, args.CommandID)
		}
		if !ok {
			time.Sleep(time.Duration(10000000))
		}
	}
	// DPrintf("Client %d PutAppend %d success\n", args.ClientID, args.CommandID)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
