package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers			[]*labrpc.ClientEnd
	lastLeader		int
	clientID		int
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
	ck.clientID = -1
	ck.nextCommandID = 1
	// Return the Clerk struct
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
	// If haven't received a client ID, obtain one first
	if ck.clientID < 0 {
		ck.clientID = ck.registerClient()
	}
	// Initialize arguments & reply struct
	args := GetArgs { key, ck.clientID, ck.nextCommandID }
	ck.nextCommandID += 1
	var reply GetReply
	ok := false
	// Loop while ok == false
	for i := ck.lastLeader; !ok; i = ck.randomServer() {
		// Send Get RPC to a server
		ok = ck.servers[i].Call("KVServer.Get", &args, &reply)
		// If RPC succeeded:
		if ok {
			if reply.WrongLeader {
				// If wrong leader, set ok = false
				ok = false
			} else if reply.Err != OK {
				// If error, return empty string
				return ""
			} else {
				// Else, update last leader
				ck.lastLeader = i
				// Return value
				return reply.Value
			}
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
	// If haven't received a client ID, obtain one first
	if ck.clientID < 0 {
		ck.clientID = ck.registerClient()
	}
	// Initialize arguments & reply struct
	args := PutAppendArgs { key, value, op, ck.clientID, ck.nextCommandID }
	ck.nextCommandID += 1
	var reply PutAppendReply
	ok := false
	// Loop while ok == false
	for i := ck.lastLeader; !ok; i = ck.randomServer() {
		// Send PutAppend RPC to a server
		ok = ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		// If RPC succeeded:
		if ok {
			if reply.WrongLeader {
				// If wrong leader, set ok = false
				ok = false
			} else {
				// Else, update last leader
				ck.lastLeader = i
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) registerClient() int {
	ok := false
	var args RegisterClientArgs
	var reply RegisterClientReply
	var i int
	for !ok {
		i = ck.randomServer()
		ok = ck.servers[i].Call("KVServer.RegisterClient", &args, &reply)
		if ok && reply.ClientID < 0 {
			ok = false
		}
	}
	ck.lastLeader = i
	return reply.ClientID
}
