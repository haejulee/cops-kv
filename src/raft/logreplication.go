package raft

import (
	"time"
)

func (rf *Raft) leaderLogReplication(term int) {
	for true {
		rf.mu.Lock()
		// If rf is killed, return
		if rf.stillAlive == false {
			rf.mu.Unlock()
			return
		}
		// Continue while rf is leader & on the same term
		if rf.currentRole != Leader || rf.CurrentTerm != term {
			rf.mu.Unlock()
			return
		}
		// Update commit index
		for commitIndex:=rf.commitIndex; commitIndex<=rf.highestLogIndex(); commitIndex++ {
			// Do only for log entries whose term is the current term:
			if rf.logEntry(commitIndex).Term == rf.CurrentTerm {
				// Count number of replications for log entry with commitIndex
				ct := 1
				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= commitIndex {
						ct += 1
					}
				}
				// If the log entry has been replicated on a majority of servers,
				// commit index & continue
				if ct > len(rf.peers)/2 {
					rf.commitIndex = commitIndex
				} else { // Else, break from for loop
					break
				}
			}
		}
		// Initiate log replications
		for i, nextIndex := range rf.nextIndex {
			// If last log index >= nextIndex
			if i != rf.me && rf.highestLogIndex() >= nextIndex {
				// Send AppendEntries with log entries starting at nextIndex
				args := &AppendEntriesArgs{
					rf.CurrentTerm,
					rf.me,
					nextIndex-1,
					rf.logEntry(nextIndex-1).Term,
					rf.commitIndex,
					rf.logSlice(nextIndex,-1),
				}
				rf.nextIndex[i] = rf.highestLogIndex() + 1
				go rf.replicateLog(i, args)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartbeatPeriod/2))
	}
}

func (rf *Raft) replicateLog(i int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	// Send AppendEntries RPC until a response is received
	for ok := false ; !ok ; time.Sleep(time.Duration(heartbeatPeriod/2)) {
		// Send appendEntries
		ok = rf.sendAppendEntries(i, args, &reply)
		rf.mu.Lock()
		if ok {
			if reply.Term > args.Term {
				// If term is greater than currentTerm, update currentTerm
				// & revert to follower
				rf.updateTerm(reply.Term)
			} else if reply.Success {
				// Else, if success returned, update rf.matchIndex[i]
				matchIndex := args.PrevLogIndex + len(args.Entries)
				if matchIndex > rf.matchIndex[i] {
					rf.matchIndex[i] = matchIndex
				}
			} else {
				// Else if success returned false, decrement rf.nextIndex[i]
				rf.nextIndex[i] = reply.ConflictIndex
			}
		}
		// If rf no longer alive, return
		if rf.stillAlive == false {
			rf.mu.Unlock()
			return
		}
		// Stop trying if no longer leader for the same term
		if rf.currentRole != Leader || rf.CurrentTerm != args.Term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}
