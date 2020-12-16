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
			if rf.logTerm(commitIndex) == rf.CurrentTerm {
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
					DPrintf("leaderLogReplication %d commitIndex %d\n", rf.me, rf.commitIndex)
				} else { // Else, break from for loop
					break
				}
			}
		}
		// Initiate log replications
		// DPrintf("leader log length %d last elem", rf.highestLogIndex(), rf.logEntry(rf.highestLogIndex()))
		for i, nextIndex := range rf.nextIndex {
			// If last log index >= nextIndex
			if i != rf.me && rf.highestLogIndex() >= nextIndex {
				if nextIndex > rf.SnapshotIndex {
					// Calculate previous index & previous term
					prevIndex := nextIndex-1
					prevTerm := rf.logTerm(prevIndex)
					// Send AppendEntries with log entries starting at nextIndex
					args := &AppendEntriesArgs{
						rf.CurrentTerm,
						rf.me,
						prevIndex,
						prevTerm,
						rf.commitIndex,
						rf.logSlice(nextIndex,-1),
					}
					rf.nextIndex[i] = rf.highestLogIndex() + 1
					go rf.replicateLog(i, args)
				} else {
					// Send InstallSnapshot
					snapshot, _ := rf.readSnapshot()
					args := &InstallSnapshotArgs{
						rf.CurrentTerm,
						rf.me,
						rf.SnapshotIndex,
						rf.SnapshotTerm,
						snapshot.Snapshot,
					}
					rf.nextIndex[i] = rf.SnapshotIndex + 1
					go rf.replicateSnapshot(i, args)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartbeatPeriod/5))
	}
}

func (rf *Raft) replicateLog(i int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	// Send AppendEntries RPC until a response is received
	for ok := false ; !ok ; time.Sleep(time.Duration(heartbeatPeriod/5)) {
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

func (rf *Raft) replicateSnapshot(i int, args *InstallSnapshotArgs) {
	var reply InstallSnapshotReply
	// Send InstallSnapshot RPC until a response is received
	for ok := false ; !ok ; time.Sleep(time.Duration(heartbeatPeriod/5)) {
		// Send appendEntries
		ok = rf.sendInstallSnapshot(i, args, &reply)
		rf.mu.Lock()
		if ok {
			if reply.Term > args.Term {
				// If term is greater than currentTerm, update currentTerm
				// & revert to follower
				rf.updateTerm(reply.Term)
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
