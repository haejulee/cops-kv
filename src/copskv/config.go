package copskv

import (
	crand "crypto/rand"
	"encoding/base64"
	"os"
	"strconv"
	"sync"

	"copsmaster"
	"labrpc"
	"raft"
)

const (
	nclusters = 2 // 2 clusters (datacenters)
	nmasters = 3 // 3 master servers per cluster
	ngroups = 3 // 3 groups per cluster
	nreplicas = 3 // 3 replicas per group

	maxraftstate = 100
)

type Config struct {
	mu    sync.Mutex
	net   *labrpc.Network

	nclusters int
	clusters []cluster

	nmasters int
	ngroups int
	n int // servers per k/v group
	maxraftstate int
}

type cluster struct {
	masterservers []*copsmaster.ShardMaster
	mck           *copsmaster.Clerk

	groups  []*group

	clerks       map[*Clerk][]string
	nextClientId int
}

type group struct {
	gid       int
	servers   []*ShardKV
	saved     []*raft.Persister
	endnames  [][]string // ends from member i to member j inside group
	mendnames [][][]string // ends from member i to cluster c's master j
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// Initializes a system with sharded clusters
func MakeSystem() *Config {
	cfg := &Config{}
	cfg.net = labrpc.MakeNetwork()

	cfg.nclusters = nclusters
	cfg.nmasters = nmasters
	cfg.ngroups = ngroups
	cfg.n = nreplicas

	cfg.maxraftstate = maxraftstate

	cfg.clusters = make([]cluster, cfg.nclusters)
	for c := 0; c < cfg.nclusters; c++ {

		cfg.clusters[c].masterservers = make([]*copsmaster.ShardMaster, cfg.nmasters)
		for i := 0; i < cfg.nmasters; i++ {
			cfg.StartMasterServer(c, i)
		}
		cfg.clusters[c].mck = cfg.shardclerk(c)

		cfg.clusters[c].groups = make([]*group, cfg.ngroups)
		for gi := 0; gi < cfg.ngroups; gi++ {
			gg := &group{}
			cfg.clusters[c].groups[gi] = gg
			gg.gid = 100 + gi
			gg.servers = make([]*ShardKV, cfg.n)
			gg.saved = make([]*raft.Persister, cfg.n)
			gg.endnames = make([][]string, cfg.n)
			gg.mendnames = make([][][]string, cfg.n)
			for i := 0; i < cfg.n; i++ {
				cfg.StartServer(c, gi, i)
			}
		}

		cfg.clusters[c].clerks = make(map[*Clerk][]string)
		cfg.clusters[c].nextClientId = cfg.n + 1000
	}

	cfg.net.Reliable(true)

	// Join all groups into clusters
	for c := 0; c < cfg.nclusters; c++ {
		cfg.joinm(c)
	}

	return cfg
}

func (cfg *Config) joinm(c int) {
	m := make(map[int][]string, cfg.ngroups)
	for g := 0; g < cfg.ngroups; g++ {
		gid := cfg.clusters[c].groups[g].gid
		servernames := make([]string, cfg.n)
		for i := 0; i < cfg.n; i++ {
			servernames[i] = cfg.servername(c, gid, i)
		}
		m[gid] = servernames
	}
	cfg.clusters[c].mck.Join(m)
}

func (cfg *Config) StartMasterServer(c int, i int) {
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, cfg.mastername(c, j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	cfg.clusters[c].masterservers[i] = copsmaster.StartServer(ends, i, p)

	msvc := labrpc.MakeService(cfg.clusters[c].masterservers[i])
	rfsvc := labrpc.MakeService(cfg.clusters[c].masterservers[i].Raft())
	srv := labrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.mastername(c, i), srv)
}

func (cfg *Config) mastername(c int, i int) string {
	return "cluster-" + strconv.Itoa(c) + "-master-" + strconv.Itoa(i)
}

func (cfg *Config) shardclerk(c int) *copsmaster.Clerk {
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.mastername(c, j))
		cfg.net.Enable(name, true)
	}
	return copsmaster.MakeClerk(ends)
}

func (cfg *Config) StartServer(c int, gi int, i int) {
	cfg.mu.Lock()

	gg := cfg.clusters[c].groups[gi]

	gg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		gg.endnames[i][j] = randstring(20)
	}

	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
		cfg.net.Connect(gg.endnames[i][j], cfg.servername(c, gg.gid, j))
		cfg.net.Enable(gg.endnames[i][j], true)
	}

	mends := make([][]*labrpc.ClientEnd, cfg.nclusters)
	gg.mendnames[i] = make([][]string, cfg.nclusters)
	for cid := 0; cid < cfg.nclusters; cid++ {
		mends[cid] = make([]*labrpc.ClientEnd, cfg.nmasters)
		gg.mendnames[i][cid] = make([]string, cfg.nmasters)
		for j := 0; j < cfg.nmasters; j++ {
			gg.mendnames[i][cid][j] = randstring(20)
			mends[cid][j] = cfg.net.MakeEnd(gg.mendnames[i][cid][j])
			cfg.net.Connect(gg.mendnames[i][cid][j], cfg.mastername(cid, j))
			cfg.net.Enable(gg.mendnames[i][cid][j], true)
		}
	}

	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	} else {
		gg.saved[i] = raft.MakePersister()
	}

	cfg.mu.Unlock()

	gg.servers[i] = StartServer(ends, i, gg.saved[i], cfg.maxraftstate, c, gg.gid, mends,
		func(servername string) *labrpc.ClientEnd {
			name := randstring(20)
			end := cfg.net.MakeEnd(name)
			cfg.net.Connect(name, servername)
			cfg.net.Enable(name, true)
			return end
		})
	
	kvsvc := labrpc.MakeService(gg.servers[i])
	rfsvc := labrpc.MakeService(gg.servers[i].rf)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.servername(c, gg.gid, i), srv)
}

func (cfg *Config) servername(c int, gid int, i int) string {
	return "cluster-" + strconv.Itoa(c) + "-server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(i)
}

// Cleanup - call after use of system
func (cfg *Config) Cleanup() {
	for c := 0; c < cfg.nclusters; c++ {
		for gi := 0; gi < cfg.ngroups; gi++ {
			cfg.ShutdownGroup(c, gi)
		}
	}
	cfg.net.Cleanup()
}

func (cfg *Config) ShutdownGroup(c int, gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.ShutdownServer(c, gi, i)
	}
}

func (cfg *Config) ShutdownServer(c int, gi int, i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	
	gg := cfg.clusters[c].groups[gi]

	for j := 0; j < len(gg.servers); j++ {
		name := gg.endnames[i][j]
		cfg.net.Enable(name, false)
	}
	for j := 0; j < len(gg.mendnames[i]); j++ {
		for k := 0; k < len(gg.mendnames[i][j]); k++ {
			name := gg.mendnames[i][j][k]
			cfg.net.Enable(name, false)
		}
	}

	cfg.net.DeleteServer(cfg.servername(c, gg.gid, i))

	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	}

	kv := gg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		gg.servers[i] = nil
	}
}


func (cfg *Config) MakeClient(c int) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	endnames := make([]string, cfg.n)
	for j := 0; j < cfg.nmasters; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], cfg.mastername(c, j))
		cfg.net.Enable(endnames[j], true)
	}

	ck := MakeClerk(ends, func(servername string) *labrpc.ClientEnd {
		name := randstring(20)
		end := cfg.net.MakeEnd(name)
		cfg.net.Connect(name, servername)
		cfg.net.Enable(name, true)
		return end
	})
	cfg.clusters[c].clerks[ck] = endnames
	cfg.clusters[c].nextClientId++
	
	return ck
}

func (cfg *Config) DeleteClient(c int, ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clusters[c].clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clusters[c].clerks, ck)
}
