//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"google.golang.org/protobuf/proto"
)

const (

	// tcpMaxPool controls how many connections we will pool
	tcpMaxPool = 3

	// tcpTimeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	tcpTimeout = 10 * time.Second

	raftDBName = "raft.db"

	// logCacheCapacity is the maximum number of logs to cache in-memory.
	// This is used to reduce disk I/O for the recently committed entries.
	logCacheCapacity = 512

	nRetainedSnapShots = 1
)

var (
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader      = errors.New("node is not the leader")
	ErrLeaderNotFound = errors.New("leader not found")
	ErrNotOpen        = errors.New("store not open")
)

// Indexer interface updates both the collection and its indices in the filesystem.
// This is distinct from updating metadata, which is handled through a different interface.
type Indexer interface {
	AddClass(cmd.AddClassRequest) error
	UpdateClass(cmd.UpdateClassRequest) error
	DeleteClass(string) error
	AddProperty(string, command.AddPropertyRequest) error
	AddTenants(class string, req *command.AddTenantsRequest) error
	UpdateTenants(class string, req *command.UpdateTenantsRequest) error
	DeleteTenants(class string, req *command.DeleteTenantsRequest) error
	UpdateShardStatus(req *command.UpdateShardStatusRequest) error
	GetShardsStatus(class string) (models.ShardStatusList, error)
}

type Parser interface {
	ParseClass(class *models.Class) error
}

type Config struct {
	WorkDir  string // raft working directory
	NodeID   string
	Host     string
	RaftPort int
	RPCPort  int

	// ServerName2PortMap maps server names to port numbers
	ServerName2PortMap map[string]int
	BootstrapExpect    int

	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	RecoveryTimeout   time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	DB           Indexer
	Parser       Parser
	AddrResolver addressResolver
	Logger       *slog.Logger
	LogLevel     string
	Voter        bool
	// IsLocalHost only required when running Weaviate from the console in localhost
	IsLocalHost bool
}

type Store struct {
	raft              *raft.Raft
	open              atomic.Bool
	raftDir           string
	raftPort          int
	bootstrapExpect   int
	recoveryTimeout   time.Duration
	heartbeatTimeout  time.Duration
	electionTimeout   time.Duration
	snapshotInterval  time.Duration
	applyTimeout      time.Duration
	snapshotThreshold uint64

	nodeID   string
	host     string
	db       *localDB
	log      *slog.Logger
	logLevel string

	bootstrapped atomic.Bool
	logStore     *raftbolt.BoltStore
	addResolver  *addrResolver
	transport    *raft.NetworkTransport

	mutex      sync.Mutex
	candidates map[string]string
}

func New(cfg Config) Store {
	return Store{
		raftDir:           cfg.WorkDir,
		raftPort:          cfg.RaftPort,
		bootstrapExpect:   cfg.BootstrapExpect,
		candidates:        make(map[string]string, cfg.BootstrapExpect),
		recoveryTimeout:   cfg.RecoveryTimeout,
		heartbeatTimeout:  cfg.HeartbeatTimeout,
		electionTimeout:   cfg.ElectionTimeout,
		snapshotInterval:  cfg.SnapshotInterval,
		snapshotThreshold: cfg.SnapshotThreshold,
		applyTimeout:      time.Second * 20,
		nodeID:            cfg.NodeID,
		host:              cfg.Host,
		addResolver:       newAddrResolver(&cfg),
		db:                &localDB{NewSchema(cfg.NodeID, cfg.DB), cfg.DB, cfg.Parser},
		log:               cfg.Logger,
		logLevel:          cfg.LogLevel,
	}
}

func (f *Store) SetDB(db DB) {
	f.db = db
	f.schema.shardReader = db
}

	if err = os.MkdirAll(st.raftDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", st.raftDir, err)
	}

	// log store
	st.logStore, err = raftbolt.NewBoltStore(filepath.Join(st.raftDir, raftDBName))
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.Command{
		Type:       command.Command_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return st.executeCommand(cmd)
}

func (st *Store) UpdateClass(cls *models.Class, ss *sharding.State) error {
	req := command.UpdateClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.Command{
		Type:       command.Command_TYPE_UPDATE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return st.executeCommand(cmd)
}

func (st *Store) DeleteClass(name string) error {
	cmd := &command.Command{
		Type:  command.Command_TYPE_DELETE_CLASS,
		Class: name,
	}
	return st.executeCommand(cmd)
}

func (st *Store) RestoreClass(cls *models.Class, ss *sharding.State) error {
	req := command.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	// tcp transport
	address := fmt.Sprintf("%s:%d", st.host, st.raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return fmt.Errorf("net.ResolveTCPAddr address=%v: %w", address, err)
	}

	st.transport, err = st.addResolver.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout)
	if err != nil {
		return fmt.Errorf("raft.NewTCPTransport  address=%v tcpAddress=%v maxPool=%v timeOut=%v: %w", address, tcpAddr, tcpMaxPool, tcpTimeout, err)
	}

	st.log.Info("raft tcp transport", "address", address, "tcpMaxPool", tcpMaxPool, "tcpTimeout", tcpTimeout)

	rLog := rLog{st.logStore}
	st.initialLastAppliedIndex, err = rLog.LastAppliedCommand()
	if err != nil {
		return fmt.Errorf("read log last command: %w", err)
	}

	if st.initialLastAppliedIndex == snapshotIndex(snapshotStore) {
		st.loadDatabase(ctx)
	}

	// raft node
	st.raft, err = raft.NewRaft(st.raftConfig(), st, logCache, st.logStore, snapshotStore, st.transport)
	if err != nil {
		return fmt.Errorf("raft.NewRaft %v %w", address, err)
	}

	st.log.Info("starting raft", "applied_index", st.raft.AppliedIndex(), "last_index",
		st.raft.LastIndex(), "last_log_index", st.initialLastAppliedIndex)

	go func() {
		lastLeader := "Unknown"
		t := time.NewTicker(time.Second * 30)
		defer t.Stop()
		for range t.C {
			leader := st.Leader()
			if leader != lastLeader {
				lastLeader = leader
				st.log.Info("current Leader", "address", lastLeader)
			}
		}
		return err
	}
	return st.executeCommand(cmd)
}

func (st *Store) AddProperty(class string, p *models.Property) error {
	req := command.AddPropertyRequest{Property: p}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.Command{
		Type:       command.Command_TYPE_ADD_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return st.executeCommand(cmd)
}

func (st *Store) UpdateShardStatus(class, shard, status string) error {
	req := command.UpdateShardStatusRequest{class, shard, status}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := command.Command{
		Type:       command.Command_TYPE_UPDATE_SHARD_STATUS,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	fut := st.raft.Apply(cmdBytes, st.raftApplyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
	}

	return nil
}

func (st *Store) AddTenants(class string, req *command.AddTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.Command{
		Type:       command.Command_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return st.executeCommand(cmd)
}

func (st *Store) UpdateTenants(class string, req *command.UpdateTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.Command{
		Type:       command.Command_TYPE_UPDATE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return st.executeCommand(cmd)
}

func (st *Store) DeleteTenants(class string, req *command.DeleteTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.Command{
		Type:       command.Command_TYPE_DELETE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return st.executeCommand(cmd)
}

func (st *Store) executeCommand(cmd *command.Command) error {
	cmdBytes, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	fut := st.raft.Apply(cmdBytes, st.applyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	return nil
}

// Join adds the given peer to the cluster.
// This operation must be executed on the leader, otherwise, it will fail with ErrNotLeader.
// If the cluster has not been opened yet, it will return ErrNotOpen.
func (st *Store) Join(id, addr string, voter bool) error {
	if !st.open.Load() {
		return ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	rID, rAddr := raft.ServerID(id), raft.ServerAddress(addr)

	if !voter {
		return st.assertFuture(st.raft.AddNonvoter(rID, rAddr, 0, 0))
	}
	return st.assertFuture(st.raft.AddVoter(rID, rAddr, 0, 0))
}

// Remove removes this peer from the cluster
func (st *Store) Remove(id string) error {
	if !st.open.Load() {
		return ErrNotOpen
	}
	if st.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	return st.assertFuture(st.raft.RemoveServer(raft.ServerID(id), 0, 0))
}

// Notify signals this Store that a node is ready for bootstrapping at the specified address.
// Bootstrapping will be initiated once the number of known nodes reaches the expected level,
// which includes this node.

func (st *Store) Notify(id, addr string) (err error) {
	if !st.open.Load() {
		return ErrNotOpen
	}
	// peer is not voter or already bootstrapped or belong to an existing cluster
	if st.bootstrapExpect == 0 || st.bootstrapped.Load() || st.Leader() != "" {
		return nil
	}

	st.mutex.Lock()
	defer st.mutex.Unlock()

	st.candidates[id] = addr
	if len(st.candidates) < st.bootstrapExpect {
		st.log.Debug("number of candidates", "expect", st.bootstrapExpect, "got", st.candidates)
		return nil
	}
	candidates := make([]raft.Server, 0, len(st.candidates))
	i := 0
	for id, addr := range st.candidates {
		candidates = append(candidates, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(addr),
		})
		delete(st.candidates, id)
		i++
	}

	st.log.Info("starting cluster bootstrapping", "candidates", candidates)

	fut := st.raft.BootstrapCluster(raft.Configuration{Servers: candidates})
	if err := fut.Error(); err != nil {
		st.log.Error("bootstrapping cluster: " + err.Error())

		return err
	}
	st.bootstrapped.Store(true)
	return nil
}

func (st *Store) assertFuture(fut raft.IndexFuture) error {
	if err := fut.Error(); err != nil && errors.Is(err, raft.ErrNotLeader) {
		return ErrNotLeader
	} else {
		return err
	}
}

func (st *Store) raftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	if st.heartbeatTimeout > 0 {
		cfg.HeartbeatTimeout = st.heartbeatTimeout
	}
	if st.electionTimeout > 0 {
		cfg.ElectionTimeout = st.electionTimeout
	}
	if st.snapshotInterval > 0 {
		cfg.SnapshotInterval = st.snapshotInterval
	}
	if st.snapshotThreshold > 0 {
		cfg.SnapshotThreshold = st.snapshotThreshold
	}
	cfg.LocalID = raft.ServerID(st.nodeID)
	cfg.LogLevel = st.logLevel
	return cfg
}

func (st *Store) loadDatabase(ctx context.Context) {
	if st.dbLoaded.Load() {
		return
	}
	if err := st.db.Load(ctx, st.nodeID); err != nil {
		st.log.Error("cannot restore database: " + err.Error())
		panic("error restoring database")
	}

	st.dbLoaded.Store(true)
	st.log.Info("database has been successfully loaded")
}

type Response struct {
	Error error
	Data  interface{}
}

var _ raft.FSM = &Store{}

func removeNilTenants(tenants []*command.Tenant) []*command.Tenant {
	n := 0
	for i := range tenants {
		if tenants[i] != nil {
			tenants[n] = tenants[i]
			n++
		}
	}
	return tenants[:n]
}
