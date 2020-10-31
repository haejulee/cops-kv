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
	Term			int
	Success			bool
	ConflictIndex	int		// Index of first entry with conflicting term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If term is less than currentTerm, return false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	// Pre-calculate current highest log index & the term corresponding to prevlogindex
	highestLogIndex := rf.highestLogIndex()
	prevLogTerm := 0
	if len(args.Entries) > 0 && highestLogIndex >= args.PrevLogIndex {
		if args.PrevLogIndex == rf.SnapshotIndex {
			prevLogTerm = rf.SnapshotTerm
		} else {
			prevLogTerm = rf.logEntry(args.PrevLogIndex).Term
		}
	}
	if len(args.Entries) > 0 &&
	   (highestLogIndex < args.PrevLogIndex || prevLogTerm != args.PrevLogTerm) {
		// If log doesn't contain entry at prevLogIndex whose term == prevLogTerm
		// return index of first element with the conflicting term
		if highestLogIndex < args.PrevLogIndex {
			// If PrevLogIndex doesn't exist in the log, return highest existing index + 1
			reply.ConflictIndex = highestLogIndex + 1
		} else {
			// Else, return lowest index with the same term as the one at PrevLogIndex
			term := rf.logEntry(args.PrevLogIndex).Term
			for i:=args.PrevLogIndex; i>=0; i-- {
				if rf.logEntry(i).Term != term {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
		// Reset election timeout
		rf.resetTimeout()
		// Return false
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else {
		changesMade := false
		// If the conditions are right for appending the entries:
		if len(args.Entries) > 0 {
			// Append any new entries not already in the log
			rf.Log = append(rf.logSlice(-1,args.PrevLogIndex), args.Entries...)
			changesMade = true
		}
		// If term > rf.CurrentTerm, update currentTerm
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.currentRole = Follower
			rf.VotedFor = -1
			changesMade = true
		}
		// Save any changes made to persistent data
		if changesMade {
			rf.persist()
		}
		// Update commit index
		highestLogIndex = rf.highestLogIndex()
		if args.LeaderCommit > highestLogIndex {
			rf.commitIndex = highestLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// Reset election timeout
		rf.resetTimeout()
		// Return true
		reply.Term = rf.CurrentTerm
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
