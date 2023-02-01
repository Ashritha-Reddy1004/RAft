package raft

import (
	"context"
	"os"
	"time"

	"github.com/shaj13/raft/internal/membership"
	"github.com/shaj13/raft/internal/raftengine"
	"github.com/shaj13/raft/internal/raftpb"
	"github.com/shaj13/raft/internal/storage"
	"github.com/shaj13/raft/internal/transport"
	"github.com/shaj13/raft/raftlog"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const None = raft.None

const (
	VoterMember MemberType = raftpb.VoterMember

	RemovedMember MemberType = raftpb.ConfChangeRemoveNode
	LearnerMember MemberType = raftpb.LearnerMember

	StagingMember MemberType = raftpb.StagingMember
)

type MemberType = raftpb.MemberType

type RawMember = raftpb.Member
type Member interface {
	ID() uint64
	Address() string
	ActiveSince() time.Time
	IsActive() bool
	Type() MemberType
	Raw() RawMember
}

type StateMachine = raftengine.StateMachine

type Option interface {
	apply(c *config)
}
type StartOption interface {
	apply(c *startConfig)
}

type startOptionFunc func(c *startConfig)

func (fn startOptionFunc) apply(c *startConfig) {
	fn(c)
}

type optionFunc func(c *config)

func (fn optionFunc) apply(c *config) {
	fn(c)
}
func WithLinearizableReadSafe() Option {
	return optionFunc(func(c *config) {

	})
}
func WithLinearizableReadLeaseBased() Option {
	return optionFunc(func(c *config) {
		c.rcfg.ReadOnlyOption = raft.ReadOnlyLeaseBased
	})
}

func WithTickInterval(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.tickInterval = d
	})
}

func WithStreamTimeOut(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.streamTimeOut = d
	})
}

func WithDrainTimeOut(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.drainTimeOut = d
	})
}

func WithStateDIR(dir string) Option {
	return optionFunc(func(c *config) {
		c.statedir = dir
	})
}

func WithMaxSnapshotFiles(max int) Option {
	return optionFunc(func(c *config) {
		c.maxSnapshotFiles = max
	})
}

func WithSnapshotInterval(i uint64) Option {
	return optionFunc(func(c *config) {
		c.snapInterval = i
	})
}

func WithElectionTick(tick int) Option {
	return optionFunc(func(c *config) {
		c.rcfg.ElectionTick = tick
	})
}

func WithHeartbeatTick(tick int) Option {
	return optionFunc(func(c *config) {
		c.rcfg.HeartbeatTick = tick
	})
}

func WithMaxSizePerMsg(max uint64) Option {
	return optionFunc(func(c *config) {
		c.rcfg.MaxSizePerMsg = max
	})
}

func WithMaxCommittedSizePerReady(max uint64) Option {
	return optionFunc(func(c *config) {
		c.rcfg.MaxCommittedSizePerReady = max
	})
}

func WithMaxUncommittedEntriesSize(max uint64) Option {
	return optionFunc(func(c *config) {
		c.rcfg.MaxUncommittedEntriesSize = max
	})
}

func WithMaxInflightMsgs(max int) Option {
	return optionFunc(func(c *config) {
		c.rcfg.MaxInflightMsgs = max
	})
}

func WithCheckQuorum() Option {
	return optionFunc(func(c *config) {
		c.rcfg.CheckQuorum = true
	})
}
func WithPreVote() Option {
	return optionFunc(func(c *config) {
		c.rcfg.PreVote = true
	})
}

func WithDisableProposalForwarding() Option {
	return optionFunc(func(c *config) {
		c.rcfg.DisableProposalForwarding = true
	})
}

func WithContext(ctx context.Context) Option {
	return optionFunc(func(c *config) {
		c.ctx = ctx
	})
}

func WithLogger(lg raftlog.Logger) Option {
	return optionFunc(func(c *config) {
		c.logger = lg
	})
}

func WithPipelining() Option {
	return optionFunc(func(c *config) {
		c.pipelining = true
	})
}
func WithJoin(addr string, timeout time.Duration) StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.Join(addr, timeout)
		c.appendOperator(opr)
	})
}
func WithForceJoin(addr string, timeout time.Duration) StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.ForceJoin(addr, timeout)
		c.appendOperator(opr)
	})
}

func WithInitCluster() StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.InitCluster()
		c.appendOperator(opr)
	})
}

func WithForceNewCluster() StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.ForceNewCluster()
		c.appendOperator(opr)
	})
}

func WithRestore(path string) StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.Restore(path)
		c.appendOperator(opr)
	})
}

func WithRestart() StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.Restart()
		c.appendOperator(opr)
	})
}

func WithMembers(membs ...RawMember) StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.Members(membs...)
		c.appendOperator(opr)
	})
}

func WithAddress(addr string) StartOption {
	return startOptionFunc(func(c *startConfig) {
		c.addr = addr
	})
}

func WithFallback(opts ...StartOption) StartOption {
	return startOptionFunc(func(c *startConfig) {

		nc := new(startConfig)
		nc.apply(opts...)

		opr := raftengine.Fallback(nc.operators...)
		c.appendOperator(opr)
	})
}

type startConfig struct {
	operators []raftengine.Operator
	addr      string
}

func (c *startConfig) appendOperator(opr raftengine.Operator) {
	c.operators = append(c.operators, opr)
}

func (c *startConfig) apply(opts ...StartOption) {
	for _, opt := range opts {
		opt.apply(c)
	}
}

type config struct {
	ctx              context.Context
	rcfg             *raft.Config
	tickInterval     time.Duration
	streamTimeOut    time.Duration
	drainTimeOut     time.Duration
	statedir         string
	maxSnapshotFiles int
	snapInterval     uint64
	groupID          uint64
	controller       transport.Controller
	storage          storage.Storage
	pool             membership.Pool
	dial             transport.Dial
	engine           raftengine.Engine
	mux              raftengine.Mux
	fsm              StateMachine
	logger           raftlog.Logger
	pipelining       bool
}

func (c *config) Logger() raftlog.Logger {
	return c.logger
}

func (c *config) Context() context.Context {
	return c.ctx
}

func (c *config) GroupID() uint64 {
	return c.groupID
}

func (c *config) TickInterval() time.Duration {
	return c.tickInterval
}

func (c *config) StreamTimeout() time.Duration {
	return c.streamTimeOut
}

func (c *config) DrainTimeout() time.Duration {
	return c.drainTimeOut
}

func (c *config) Snapshotter() storage.Snapshotter {
	return c.storage.Snapshotter()
}

func (c *config) StateDir() string {
	return c.statedir
}

func (c *config) MaxSnapshotFiles() int {
	return c.maxSnapshotFiles
}

func (c *config) Controller() transport.Controller {
	return c.controller
}

func (c *config) Storage() storage.Storage {
	return c.storage
}

func (c *config) SnapInterval() uint64 {
	return c.snapInterval
}

func (c *config) RaftConfig() *raft.Config {
	return c.rcfg
}

func (c *config) Pool() membership.Pool {
	return c.pool
}

func (c *config) Dial() transport.Dial {
	return c.dial
}

func (c *config) Reporter() membership.Reporter {
	return c.engine
}

func (c *config) StateMachine() raftengine.StateMachine {
	return c.fsm
}

func (c *config) Mux() raftengine.Mux {
	return c.mux
}

func (c *config) AllowPipelining() bool {
	return c.pipelining
}

func newConfig(opts ...Option) *config {
	c := &config{
		rcfg: &raft.Config{
			ElectionTick:              10,
			HeartbeatTick:             1,
			MaxSizePerMsg:             1024 * 1024,
			MaxInflightMsgs:           256,
			MaxUncommittedEntriesSize: 1 << 30,
		},
		ctx:              context.Background(),
		tickInterval:     time.Millisecond * 100,
		streamTimeOut:    time.Second * 10,
		drainTimeOut:     time.Second * 10,
		maxSnapshotFiles: 5,
		snapInterval:     1000,
		logger:           raftlog.DefaultLogger,
		statedir:         os.TempDir(),
		pipelining:       false,
	}

	for _, opt := range opts {
		opt.apply(c)
	}

	return c
}
