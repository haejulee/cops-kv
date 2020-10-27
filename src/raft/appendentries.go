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
	if args.Term < rf.CurrentTerm {
		// If term is less than currentTerm, return false
		reply.Term = rf.CurrentTerm
		reply.Success = false
	} else if len(rf.Log) <= args.PrevLogIndex ||
		rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// If log doesn't contain entry at prevLogIndex whose term == prevLogTerm
		// Find index of first element with the conflicting term
		if len(rf.Log) <= args.PrevLogIndex {
			reply.ConflictIndex = len(rf.Log)
		} else {
			term := rf.Log[args.PrevLogIndex].Term
			for i:=args.PrevLogIndex; i>=0; i-- {
				if rf.Log[i].Term != term {
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
		// Append any new entries not already in the log
		rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
		// If term > rf.CurrentTerm, update currentTerm
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.currentRole = Follower
			rf.VotedFor = -1
		}
		rf.persist()	// Persist changes made in lines 46-51
		// Update commit index
		if args.LeaderCommit >= len(rf.Log) {
			rf.commitIndex = len(rf.Log)-1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		// Reset election timeout
		rf.resetTimeout()
		// Return true
		reply.Term = rf.CurrentTerm
		reply.Success = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
