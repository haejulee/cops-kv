package copskv


// Extracts just the Lamport Timestamp from a version number
func vtot(versionNum uint64) uint32 {
	return uint32(versionNum >> 32)
}

// Returns true if v1 is at least as late as v2
func versionUpToDate(v1, v2 uint64) bool {
	// Convert to lamport timestamps
	t1 := vtot(v1)
	t2 := vtot(v2)
	// compare timestamps
	if t1 >= t2 {
		return true
	} else {
		return false
	}
}

// Returns Lamport timestamp of next event
// Must hold lock while calling
func (kv *ShardKV) lamportTimestamp() uint32 {
	// Add 1 to latest timestamp & return its value
	kv.latestTimestamp += 1
	return kv.latestTimestamp
}

// Updates system's latest witnessed timestamp,
// considering an incoming timestamp
// Must hold lock while calling
func (kv *ShardKV) updateTimestamp(incoming uint32) {
	if incoming > kv.latestTimestamp {
		kv.latestTimestamp = incoming
	}
}

// Returns a next version number based on Lamport Timestamp
// Must hold lock while calling
func (kv *ShardKV) versionNumber() uint64 {
	timestamp := uint64(kv.lamportTimestamp())
	ver := timestamp << 32
	ver = ver | uint64(kv.nodeID)
	return ver
}
