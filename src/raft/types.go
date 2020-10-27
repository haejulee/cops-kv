package raft

import (
	"sync"

	"labrpc"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// Mutex lock
	mu			sync.Mutex			// Lock to protect shared access to this peer's state
	
	// State that never gets modified past startup (no lock required to access)
	peers		[]*labrpc.ClientEnd	// RPC end points of all peers
	persister	*Persister			// Object to hold this peer's persisted state
	me			int					// this peer's index into peers[]
	
	// Election timeout "cancel button"
	timeoutCanceled	*timeoutCanceledBool // Struct shared with most recent timeout goroutine

	// Persistent state on all servers
	CurrentTerm	int				// Latest term server has seen (increases monotonically)
	VotedFor	int				// ID of candidate that received server's vote in current term
	Log			[]logEntry		// Log (log entries)

	// Volatile state on all servers
	commitIndex	int				// Index of highest known committed entry (increases monotonically)
	lastApplied	int				// Index of highest log entry applied (increases monotonically)
	currentRole	role			// Role the server thinks it currently has

	// Volatile state on leaders
	nextIndex	[]int			// Index of next log entry to send to each server
	matchIndex	[]int			// Index of highest log entry known to be replicated (increases monotonically)
}

type timeoutCanceledBool struct {
	isCanceled bool
}

type logEntry struct {
	Term	int
	Command	interface{}
}

type role uint8

const (
	Follower 	role = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

