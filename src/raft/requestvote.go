package raft

type RequestVoteArgs struct {
	Term,
	CandidateID,
	LastLogIndex,
	LastLogTerm		int
}

type RequestVoteReply struct {
	Term		int
	VoteGranted	bool
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term < rf.CurrentTerm {
		// If candidate's term < currentTerm, return false
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			// If candidate's term > currentTerm:
			// become follower, update currentTerm, and reset votedFor
			rf.currentRole = Follower
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}
		// Grant vote only if server hasn't voted yet and candidate's log is
		// at least as up-to-date as server's
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		lastLogIndex := rf.highestLogIndex()
		var lastLogTerm int
		if lastLogIndex == rf.SnapshotIndex {
			lastLogTerm = rf.SnapshotTerm
		} else {
			lastLogTerm = rf.logEntry(lastLogIndex).Term
		}
		if rf.VotedFor == -1 &&
			(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm &&
			args.LastLogIndex >= lastLogIndex)) {
			// Grant vote
			rf.VotedFor = args.CandidateID
			reply.VoteGranted = true
			// Reset timeout
			rf.resetTimeout()
		}
	}
	rf.persist()	// Persist changes made in this function
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
