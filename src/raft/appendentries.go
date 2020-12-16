package raft

import "reflect"

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
	// If PrevLogIndex is less than SnapshotIndex, return ConflictIndex=commitIndex
	if args.PrevLogIndex < rf.SnapshotIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.ConflictIndex = rf.commitIndex + 1 // "try from rf.commitIndex+1 next time"
		return
	}
	// Pre-calculate current highest log index & the term corresponding to prevlogindex
	highestLogIndex := rf.highestLogIndex()
	prevLogTerm := 0
	if len(args.Entries) > 0 && highestLogIndex >= args.PrevLogIndex {
		prevLogTerm = rf.logTerm(args.PrevLogIndex)
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
			term := rf.logTerm(args.PrevLogIndex)
			for i:=args.PrevLogIndex; i>=rf.SnapshotIndex; i-- {
				if i == rf.SnapshotIndex {
					reply.ConflictIndex = i
					break
				}
				if rf.logTerm(i) != term {
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
			if rf.highestLogIndex() <= (args.PrevLogIndex + len(args.Entries)) {
				// If log contains less entries than it would with the given entries,
				// append clean copy of given entries to Log[:args.PrevLogIndex]
				rf.Log = append(rf.logSlice(-1,args.PrevLogIndex), args.Entries...)
			} else {
				// Else, first check if existing entries match given entries
				// If not a perfect match, discard entries that are more ahead
				notMatch := false
				for i := 1; i <= len(args.Entries); i++ {
					existing := rf.logEntry(args.PrevLogIndex + i)
					new := args.Entries[i - 1]
					if existing.Term != new.Term || !reflect.DeepEqual(existing.Command, new.Command) {
						notMatch = true
						DPrintf("not match:", existing, new)
						break
					}
				}
				if notMatch {
					rf.Log = append(rf.logSlice(-1,args.PrevLogIndex), args.Entries...)
				}
			}
			DPrintf("AppendEntries %d log length %d\n", rf.me, len(rf.Log))
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
		// Update commit index if args.LeaderCommit is news
		// note: it may be old news if follower's latest applied index has
		// already surpassed the known commit index
		// ALSO only do this if it's not a heartbeat message
		if args.LeaderCommit > rf.commitIndex && args.PrevLogIndex >= 0 {
			highestLogIndex = rf.highestLogIndex()
			if args.LeaderCommit > highestLogIndex {
				rf.commitIndex = highestLogIndex
				DPrintf("AppendEntries1 %d commitIndex %d\n", rf.me, rf.commitIndex)
			} else {
				rf.commitIndex = args.LeaderCommit
				DPrintf("AppendEntries2 %d commitIndex %d highestLogIndex %d\n", rf.me, rf.commitIndex, highestLogIndex)
			}
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
