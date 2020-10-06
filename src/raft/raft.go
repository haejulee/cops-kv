package raft

import (
	"bytes"
	"log"
	"time"

	"labgob"
	"labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	term = rf.CurrentTerm
	isLeader = rf.currentRole == Leader
	rf.mu.Unlock()
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(len(rf.Log))
	for i := range rf.Log {
		e.Encode(rf.Log[i])
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm, VotedFor, nLogs int
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&nLogs) != nil {
		log.Fatal("failed to decode persistent state")
	}
	rf.CurrentTerm = CurrentTerm
	rf.VotedFor = VotedFor
	rf.Log = make([]logEntry, nLogs)
	for i:=0; i<nLogs; i++ {
		var entry logEntry
		if d.Decode(&entry) != nil {
			log.Fatal("failed to decode persistent state")
		}
		rf.Log[i].Term = entry.Term
		rf.Log[i].Command = entry.Command
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	// If rf isn't leader, return false
	if rf.currentRole != Leader {
		rf.mu.Unlock()
		return 0, 0, false
	}
	// Get next index & current term
	index := len(rf.Log)
	term := rf.CurrentTerm
	// Create log entry for command
	entry := logEntry{term, command}
	// Append new log entry to log
	rf.Log = append(rf.Log, entry)
	// Persist changes to log
	rf.persist()
	rf.mu.Unlock()
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.cancelTimeout()
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Initialize persistent state
	rf.CurrentTerm = 0			// Start at term 0
	rf.VotedFor = -1			// Null value of votedFor is -1
	rf.Log = []logEntry{		// Initialize log with dummy entry at index 0
		logEntry{0,nil},		// (first log entry index is 1)
	}

	// Initialize volatile state
	rf.commitIndex = 0			// Start at commitIndex 0
	rf.lastApplied = 0			// Start at lastApplied 0
	rf.currentRole = Follower	// Start as Follower

	// Initialize leader state arrays
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start background routine that will start elections after timeout
	rf.resetTimeout()
	go rf.electionTimeoutChecker()

	// Start background routine that will apply commands as they're committed
	go rf.applyCommands(applyCh)

	return rf
}

func (rf *Raft) applyCommands(applyCh chan ApplyMsg) {
	for true {
		rf.mu.Lock()
		var i int
		for rf.lastApplied < rf.commitIndex {
			i = rf.lastApplied + 1
			msg := ApplyMsg{true, rf.Log[i].Command, i}
			applyCh <- msg
			rf.lastApplied = i
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartbeatPeriod))
	}
}
