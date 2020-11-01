/* leaderelection.go
All functions related to leader election & heartbeats, which get executed
in the background as goroutines.
*/

package raft

import (
	"math/rand"
	"sync"
	"time"
)

const (
	minElectionTimeout int64 =  400000000	// 0.4 sec
	maxElectionTimeout int64 =  800000000	// 0.8 sec
	heartbeatPeriod    int64 =  100000000	// 0.1 sec
	timeoutCheckPeriod int64 =   50000000	// 0.05 sec
)

func randomizedElectionTimeout() time.Duration {
	duration := rand.Int63n(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
	return time.Duration(duration)
}

func (rf *Raft) cancelTimeout() {
	rf.timeoutActive = false
}

func (rf *Raft) resetTimeout() {
	rf.electionTimeout = randomizedElectionTimeout()
	rf.lastTimeoutReset = time.Now()
	rf.timeoutActive = true
}

// Background procedure for starting new elections when election timeout is due
func (rf *Raft) electionTimeoutChecker() {
	for {
		rf.mu.Lock()
		if rf.stillAlive == false { // Terminate if raft instance is killed
			rf.mu.Unlock()
			return
		}
		if rf.timeoutActive &&
		   time.Since(rf.lastTimeoutReset) > rf.electionTimeout {
			rf.leaderElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(timeoutCheckPeriod))
	}
}

// Subroutine of electionTimeoutChecker, runs when election timeout is due
func (rf *Raft) leaderElection() {
	// Start new term as candidate
	rf.CurrentTerm += 1
	rf.currentRole = Candidate
	// DPrintf("Raft %d starting election %d\n", rf.me, rf.CurrentTerm)
	// Vote for self
	rf.VotedFor = rf.me
	// Persist changes made to current term & voted for
	rf.persist()
	// Create RequestVote args
	lastLogIndex := rf.highestLogIndex()
	var lastLogTerm int
	if lastLogIndex == rf.SnapshotIndex {
		lastLogTerm = rf.SnapshotTerm
	} else {
		lastLogTerm = rf.logEntry(lastLogIndex).Term
	}
	args := &RequestVoteArgs{
		rf.CurrentTerm,			// Candidate's term
		rf.me,					// Candidate's ID
		lastLogIndex,			// Index of candidate's last log entry
		lastLogTerm,			// Term of candidate's last log entry
	}
	// Set new election timeout
	rf.resetTimeout()
	// Send out RequestVotes RPCs
	go rf.requestVotes(args)
}

// Independent procedure for starting & processing a new election,
// triggered by leaderElection in electionTimeoutChecker
func (rf *Raft) requestVotes(args *RequestVoteArgs) {
	// Initialize vote counter
	ctr := &voteCounter{}
	ctr.votedFor = 1
	// Request votes from peers in parallel
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, args, ctr)
		}
	}
	// Wait until majority vote acquired
	for true {
		// If majority vote acquired, break from loop
		ctr.mu.Lock()
		if ctr.votedFor > len(rf.peers)/2 {
			ctr.mu.Unlock()
			break
		}
		ctr.mu.Unlock()
		rf.mu.Lock()
		// If rf has been killed, return
		if rf.stillAlive == false {
			rf.mu.Unlock()
			return
		}
		// If rf is past this election's term, return
		// If rf isn't a candidate anymore, also return
		if rf.CurrentTerm > args.Term || rf.currentRole != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// Sleep to prevent livelock
		time.Sleep(time.Duration(heartbeatPeriod/2))
	}
	// Once majority vote has been received:
	rf.mu.Lock()
	// While holding lock, make sure again that the election won hasn't passed
	// its term.
	if rf.CurrentTerm == args.Term {
		// Cancel election timeout to start term as leader
		rf.cancelTimeout()
		// Become leader
		rf.currentRole = Leader
		DPrintf("Raft %d became leader, term %d\n", rf.me, rf.CurrentTerm)
		for i := range rf.peers {
			// Initialize leader volatile state
			rf.nextIndex[i] = rf.highestLogIndex() + 1 // last log index + 1
			rf.matchIndex[i] = 0
		}
		// Save the term for which rf became leader
		leaderTerm := rf.CurrentTerm
		// Start log replication on leader
		go rf.leaderLogReplication(leaderTerm)
		// Let go of lock
		rf.mu.Unlock()
		// Start sending out periodic heartbeats
		rf.leaderHeartbeats(leaderTerm)
	} else {
		rf.mu.Unlock()
	}
}

type voteCounter struct {
	votedFor 		int
	mu				sync.Mutex	// Must be acquired before modifying votedFor
}

// Procedure for sending RequestVote RPC to a single peer
func (rf *Raft) requestVote(i int, args *RequestVoteArgs, ctr *voteCounter) {
	var reply RequestVoteReply
	// Send request to peer until you get a response
	for ok := false; !ok ; time.Sleep(time.Duration(heartbeatPeriod)) {
		rf.mu.Lock()
		// If rf is no longer alive, return
		if rf.stillAlive == false {
			rf.mu.Unlock()
			return
		}
		// If rf is past this election's term, or if rf is no longer candidate
		if rf.CurrentTerm != args.Term || rf.currentRole != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok = rf.sendRequestVote(i, args, &reply)
	}
	// If peer voted for rf, increment votedFor
	if reply.VoteGranted {
		ctr.mu.Lock()
		ctr.votedFor += 1
		ctr.mu.Unlock()
	}
	// If peer has a term greater than election term:
	if reply.Term > args.Term {
		rf.mu.Lock()
		// If reply.Term is greater than rf's current term, update current term
		rf.updateTerm(reply.Term)
		rf.mu.Unlock()
	}
}

// Subroutine of requestVotes,
// Runs if rf wins a majority vote & becomes leader.
func (rf *Raft) leaderHeartbeats(term int) {
	for true {
		rf.mu.Lock()
		// If rf no longer alive, return
		if rf.stillAlive == false {
			rf.mu.Unlock()
			return
		}
		// Send out heartbeats
		if rf.CurrentTerm == term && rf.currentRole == Leader {
			// DPrintf("Raft %d sending heartbeats\n", rf.me)
			for i := range rf.peers {
				if i != rf.me {
					args := &AppendEntriesArgs{			// Heartbeat request:
						rf.CurrentTerm,					// Leader term
						rf.me,							// Leader ID
						-1,								// Ignore value
						-1,								// Ignore value
						rf.commitIndex,					// Leader's commitIndex
						[]logEntry{},					// No log entries
					}
					go rf.sendHeartbeat(i, args)
				}
			}
		} else {
			// If term has passed or no longer leader, break from loop
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// Sleep until next heartbeat time
		time.Sleep(time.Duration(heartbeatPeriod))
	}
}

// Procedure for sending a heartbeat to a single peer
func (rf *Raft) sendHeartbeat(i int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(i, args, &reply)
	if ok {
		rf.mu.Lock()
		// Check term of the reply
		if reply.Term > args.Term {
			// If reply.Term is greater than current term, update current term
			rf.updateTerm(reply.Term)
		} else if !reply.Success {
			// If otherwise returned false, decrement rf.nextIndex[i]
			rf.nextIndex[i] = reply.ConflictIndex
		}
		rf.mu.Unlock()
	}
}

// Used by sendHeartbeat & requestVote calls
func (rf *Raft) updateTerm(newTerm int) {
	// If newly received term is higher than current term, update current term
	if newTerm > rf.CurrentTerm {
		// Initialize as a new follower in the new term
		rf.CurrentTerm = newTerm
		rf.currentRole = Follower
		rf.VotedFor = -1
		// Persist changes made to current term & voted for
		rf.persist()
		// Reset (or start new, if leader) election timeout
		rf.resetTimeout()
	}
}
