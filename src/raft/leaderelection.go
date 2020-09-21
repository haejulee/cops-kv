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
	minElectionTimeout int64 =  500000000	// 0.5 sec
	maxElectionTimeout int64 = 1000000000	// 1.0 sec
	heartbeatPeriod    int64 =  100000000	// 0.1 sec
)

func randomizedElectionTimeout() time.Duration {
	duration := rand.Int63n(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
	return time.Duration(duration)
}

func (rf *Raft) resetTimeout() {
	rf.cancelTimeout()
	rf.startNewTimeout()
}

func (rf *Raft) cancelTimeout() {
	rf.timeoutCanceled.isCanceled = true
}

func (rf *Raft) startNewTimeout() {
	newTimeoutBool := &timeoutCanceledBool{false}
	rf.timeoutCanceled = newTimeoutBool
	go rf.electionTimeout(newTimeoutBool)
}

func (rf *Raft) electionTimeout(tc *timeoutCanceledBool) {
	// Sleep for random election timeout time
	time.Sleep(randomizedElectionTimeout())
	rf.mu.Lock()
	// If after timeout time, timeout hasn't been canceled & rf isn't
	// already a leader, start a new election
	if !tc.isCanceled && rf.currentRole != Leader {
		rf.leaderElection()
	}
	rf.mu.Unlock()
}

func (rf *Raft) leaderElection() {
	// Start new term as candidate
	rf.currentTerm += 1
	rf.currentRole = Candidate
	// Vote for self
	rf.votedFor = rf.me
	// Create RequestVote args
	args := &RequestVoteArgs{
		rf.currentTerm,				// Candidate's term
		rf.me,						// Candidate's ID
		len(rf.log)-1,				// Index of candidate's last log entry
		rf.log[len(rf.log)-1].Term,	// Term of candidate's last log entry
	}
	// Set new election timeout
	rf.startNewTimeout()
	// Send out RequestVotes RPCs
	rf.mu.Unlock()
	rf.requestVotes(args)
	rf.mu.Lock()
}

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
		// If rf is past this election's term, return
		// If rf isn't a candidate anymore, also return
		rf.mu.Lock()
		if rf.currentTerm > args.Term || rf.currentRole != Candidate {
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
	if rf.currentTerm == args.Term {
		// Cancel election timeout to start term as leader
		rf.cancelTimeout()
		// Become leader
		rf.currentRole = Leader
		for i := range rf.peers {
			// Initialize leader volatile state
			rf.nextIndex[i] = len(rf.log)	// last log index + 1
			rf.matchIndex[i] = 0
		}
		// Save the term for which rf became leader, then let go of lock
		leaderTerm := rf.currentTerm
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

func (rf *Raft) requestVote(i int, args *RequestVoteArgs, ctr *voteCounter) {
	var reply RequestVoteReply
	// Send request to peer until you get a response
	for ok := false; !ok ; time.Sleep(time.Duration(heartbeatPeriod)) {
		// If rf is past this election's term, or if rf is no longer candidate
		rf.mu.Lock()
		if rf.currentTerm != args.Term || rf.currentRole != Candidate {
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

func (rf *Raft) leaderHeartbeats(term int) {
	for true {
		// Send out heartbeats
		rf.mu.Lock()
		if rf.currentTerm == term && rf.currentRole == Leader {
			for i := range rf.peers {
				if i != rf.me {
					args := &AppendEntriesArgs{			// Heartbeat request:
						rf.currentTerm,					// Leader term
						rf.me,							// Leader ID
						rf.nextIndex[i]-1,				// prev log index
						rf.log[rf.nextIndex[i]-1].Term,	// prev log term
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

func (rf *Raft) sendHeartbeat(i int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(i, args, &reply)
	if ok {
		// For heartbeating, only check term of reply (TODO: should this be changed?)
		if reply.Term > args.Term {
			rf.mu.Lock()
			// If reply.Term is greater than current term, update current term
			rf.updateTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) updateTerm(newTerm int) {
	// If newly received term is higher than current term, update current term
	if newTerm > rf.currentTerm {
		// Initialize as a new follower in the new term
		rf.currentTerm = newTerm
		rf.currentRole = Follower
		rf.votedFor = -1
		// Reset (or start new, if leader) election timeout
		rf.resetTimeout()
	}
}
