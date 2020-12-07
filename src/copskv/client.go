package copskv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the copsmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "copsmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= copsmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *copsmaster.Clerk
	config   copsmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID      int64
	nextCommandID uint8
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call copsmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	DPrintf("creating clerk\n")
	ck := new(Clerk)
	ck.sm = copsmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.config = ck.sm.Query(-1)
	ck.clientID = nrand()
	ck.nextCommandID = 1
	DPrintf("created clerk\n")
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("sending get request for key %s\n", key)
	args := GetArgs{ key, ck.clientID, ck.nextCommandID }
	ck.nextCommandID += 1

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("get success")
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("get failed wrong group")
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("sending putappend request for key %s\n", key)
	args := PutAppendArgs{ key, value, op, ck.clientID, ck.nextCommandID }
	ck.nextCommandID += 1

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		// DPrintf("key %s has shard %d, for gid %d\n", key, shard, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok {
					DPrintf("putappend failed connection\n")
				}
				if ok && reply.WrongLeader == false && reply.Err == OK {
					DPrintf("putappend success\n")
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("putappend failed wrong group\n")
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
		// DPrintf("config number %d\n", ck.config.Num)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
