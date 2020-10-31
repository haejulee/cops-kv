package raft

import (
	"bytes"
	"log"

	"labgob"
)

type RaftSnapshot struct {
	Snapshot interface{}
	LastIncludedIndex,
	LastIncludedTerm int
}

func (rf *Raft) encodeSnapshot(snapshot RaftSnapshot) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	return w.Bytes()
}

func (rf *Raft) readSnapshot() (RaftSnapshot, bool) {
	data := rf.persister.ReadSnapshot()
	if len(data) == 0 {
		return RaftSnapshot{}, false
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot RaftSnapshot
	if d.Decode(&snapshot) != nil {
		log.Fatal("failed to decode snapshot")
	}
	return snapshot, true
}
