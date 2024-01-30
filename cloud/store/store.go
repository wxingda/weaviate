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
	"github.com/weaviate/weaviate/usecases/cluster"
	"google.golang.org/protobuf/proto"
)

const (

	// tcpMaxPool controls how many connections we will pool
	tcpMaxPool = 3

	// tcpTimeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
	// the timeout by (SnapshotSize / TimeoutScale).
	tcpTimeout = 10 * time.Second

	raftDBName = "raft.db"

	peersFileName = "peers.json"

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
	WorkDir         string // raft working directory
	NodeID          string
	Host            string
	RaftPort        int
	BootstrapExpect int

	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	RecoveryTimeout   time.Duration
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64

	DB     DB
	Parser Parser
}

type Store struct {
	raft    *raft.Raft
	cluster cluster.Reader

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

	nodeID string
	host   string
	schema *schema
	db     DB
	parser Parser

	bootstrapped atomic.Bool

	mutex      sync.Mutex
	candidates map[string]string
}

func New(cfg Config, cluster cluster.Reader) Store {
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
		cluster:           cluster,
		db:                localDB{NewSchema(cfg.NodeID, cfg.DB), cfg.DB, cfg.Parser},
		log:               cfg.Logger,
		logLevel:          cfg.LogLevel,
	}
}

func (f *Store) SetDB(db DB) {
	f.db = db
	f.schema.shardReader = db
}

	if err := st.genPeersFileFromBolt(
		filepath.Join(st.raftDir, raftDBName),
		filepath.Join(st.raftDir, peersFileName)); err != nil {
		return err
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

	st.transport, err = raft.NewTCPTransport(address, tcpAddr, tcpMaxPool, tcpTimeout, os.Stdout)
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

	existedConfig, err := st.configViaPeers(filepath.Join(st.raftDir, peersFileName))
	if err != nil {
		return err
	}

	if servers, recover := st.recoverable(existedConfig.Servers); recover {
		st.log.Info("recovery: start recovery with",
			"peers", servers,
			"snapshot_index", snapshotIndex(snapshotStore),
			"last_applied_log_index", st.initialLastAppliedIndex)

		if err := raft.RecoverCluster(st.raftConfig(), st, logCache, st.logStore, snapshotStore, st.transport, raft.Configuration{
			Servers: servers,
		}); err != nil {
			return fmt.Errorf("raft recovery failed: %w", err)
		}

		// load the database if <= because RecoverCluster() will implicitly
		// commits all entries in the previous Raft log before starting
		if st.initialLastAppliedIndex <= snapshotIndex(snapshotStore) {
			st.loadDatabase(ctx)
		}

		st.log.Info("recovery: succeeded from previous configuration",
			"peers", servers,
			"snapshot_index", snapshotIndex(snapshotStore),
			"last_applied_log_index", st.initialLastAppliedIndex)
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

func (f *Store) SchemaReader() *schema {
	return f.schema
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
