package raft

import (
	"bytes"
	"log"

	"labgob"
)

type InstallSnapshotArgs struct {
	Term,
	LeaderID,
	LastIncludedIndex,
	LastIncludedTerm int
	Snapshot interface{}
}

type InstallSnapshotReply struct {
	Term int
}

type RaftSnapshot struct {
	Snapshot interface{}
	LastIncludedIndex,
	LastIncludedTerm int
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// DPrintf("Raft %d install snapshot index %d\n", rf.me, args.LastIncludedIndex)
	reply.Term = rf.CurrentTerm
	// If term is less than current term, return immediately
	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}
	// If the snapshot has already been done, return immediately
	if rf.SnapshotIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	// If an existing log entry has same index & term as snapshot's last included entry,
	// retain log entries following it (if there are any log entries following it)
	if rf.highestLogIndex() > args.LastIncludedIndex &&
	   rf.logEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.Log = rf.logSlice(args.LastIncludedIndex+1, -1)
		DPrintf("InstallSnapshot1 %d log length %d\n", rf.me, len(rf.Log))
	} else { // Else, discard the entire log
		rf.Log = []logEntry{}
		DPrintf("InstallSnapshot2 %d log length %d\n", rf.me, len(rf.Log))
	}
	// Update rf.SnapshotIndex and rf.SnapshotTerm
	rf.SnapshotIndex = args.LastIncludedIndex
	DPrintf("InstallSnapshot %d SnapshotIndex %d\n", rf.me, rf.SnapshotIndex)
	rf.SnapshotTerm = args.LastIncludedTerm
	// Save updated state & snapshot to persistent storage
	state := rf.encodePersistentState()
	snapshot := RaftSnapshot{args.Snapshot, args.LastIncludedIndex, args.LastIncludedTerm}
	rf.persister.SaveStateAndSnapshot(state, rf.encodeSnapshot(snapshot))
	// Update rf.commitIndex if necessary
	if rf.commitIndex < rf.SnapshotIndex {
		rf.commitIndex = rf.SnapshotIndex
		DPrintf("InstallSnapshot %d commitIndex %d\n", rf.me, rf.commitIndex)
	}
	// Apply snapshot to applyCh if necessary
	if rf.lastApplied < args.LastIncludedIndex {
		msg := ApplyMsg{ false, args.Snapshot, -1 }
		rf.lastApplied = args.LastIncludedIndex
		rf.mu.Unlock()
		rf.applyCh <- msg
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


// Invoked by server utilizing Raft to save a snapshot to Raft server's persistent state.
// snapshot: snapshot of service state - type depends on what service is using Raft.
// index: log index of the last command that was applied to the service state.
func (rf *Raft) Snapshot(snapshot interface{}, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("Raft %d snapshotting at index %d\n", rf.me, index)
	// If there's nothing to snapshot, return immediately
	if index <= rf.SnapshotIndex {
		return
	}
	// Record term of last entry in the snapshot
	rf.SnapshotTerm = rf.logEntry(index).Term
	// Truncate the raft log to reflect snapshot
	if rf.highestLogIndex() > index {
		rf.Log = rf.logSlice(index+1, -1)
		DPrintf("Snapshot1 %d log length %d\n", rf.me, len(rf.Log))
	} else {
		rf.Log = []logEntry{}
		DPrintf("Snapshot2 %d log length %d\n", rf.me, len(rf.Log))
	}
	// Record index of last entry in the snapshot
	rf.SnapshotIndex = index
	DPrintf("Snapshot %d SnapshotIndex %d\n", rf.me, rf.SnapshotIndex)
	// Create a new Raft snapshot wrapping the service snapshot
	rss := RaftSnapshot{snapshot, rf.SnapshotIndex, rf.SnapshotTerm}
	// Save snapshot & new raft state to persistent storage
	rf.persister.SaveStateAndSnapshot(rf.encodePersistentState(), rf.encodeSnapshot(rss))
}

// Returns True if the size of Raft state currently in persistent storage in bytes is close to maxraftstate
func (rf *Raft) StateSizeLimitReached(maxraftstate int) bool {
	if maxraftstate == -1 {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO: fix. for now, it just returns true if raft state has reached max size
	if rf.persister.RaftStateSize() >= maxraftstate {
		return true
	}
	return false
}

func (rf *Raft) encodeSnapshot(snapshot RaftSnapshot) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	return w.Bytes()
}

func (rf *Raft) readSnapshot() (RaftSnapshot, bool) {
	data := rf.persister.ReadSnapshot()
	if len(data) == 0 {
		return RaftSnapshot{}, false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot RaftSnapshot
	if d.Decode(&snapshot) != nil {
		log.Fatal("failed to decode snapshot")
	}
	return snapshot, true
}
