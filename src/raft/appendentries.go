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
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		// If term is less than currentTerm, return false
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if len(rf.log) <= args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// If log doesn't contain entry at prevLogIndex whose term == prevLogTerm
		// Reset election timeout
		rf.resetTimeout()
		// Return false
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// Append any new entries not already in the log
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		// If term > rf.currentTerm, update currentTerm
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.currentRole = Follower
			rf.votedFor = -1
		}
		// Update commit index
		if args.LeaderCommit >= len(rf.log) {
			rf.commitIndex = len(rf.log)-1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// Reset election timeout
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
