package raft

type AppendEntriesArgs struct {
	Term,
	LeaderID,
	PrevLogIndex,
	PrevLogTerm,
	LeaderCommit	int
	Entries			[]logEntry
}

type AppendEntriesReply struct {
	Term	int
	Success	bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// For now, just reset timer & return true if term up to date
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// If term is less than currentTerm, return false
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// Else if term is at least as large as currentTerm
		if args.Term > rf.currentTerm {
			// If term > currentTerm, update currentTerm
			rf.currentTerm = args.Term
			rf.currentRole = Follower
			rf.votedFor = -1
		}
		// Reset timer
		rf.resetTimeout()
		// Return true
		reply.Term = rf.currentTerm
		reply.Success = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
