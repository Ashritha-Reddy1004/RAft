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
)

// None is a placeholder node ID used to identify non-existence.
const None = raft.None

const (
	// VoterMember participate in elections and log entry commitment, It is the default type.
	VoterMember MemberType = raftpb.VoterMember
	// RemovedMember represents an removed raft node.
	RemovedMember MemberType = raftpb.RemovedMember
	// LearnerMember will receive log entries, but it won't participate in elections or log entry commitment.
	LearnerMember MemberType = raftpb.LearnerMember
	// StagingMember will receive log entries, but it won't participate in elections or log entry commitment,
	// and once it receives enough log entries to be sufficiently caught up to
	// the leader's log, the leader will promote him to VoterMember.
	StagingMember MemberType = raftpb.StagingMember
)

// MemberType used to distinguish members (voter, learner, etc).
type MemberType = raftpb.MemberType

// RawMember represents a raft cluster member and holds its metadata.
type RawMember = raftpb.Member

// Member represents a raft cluster member.
type Member interface {
	ID() uint64
	Address() string
	ActiveSince() time.Time
	IsActive() bool
	Type() MemberType
	Raw() RawMember
}

// StateMachine define an interface that must be implemented by
// application to make use of the raft replicated log.
type StateMachine = raftengine.StateMachine

// Option configures raft node using the functional options paradigm popularized by Rob Pike and Dave Cheney.
// If you're unfamiliar with this style,
// see https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html and
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis.
type Option interface {
	apply(c *config)
}

// StartOption configures how we start the raft node using the functional options paradigm ,
// popularized by Rob Pike and Dave Cheney. If you're unfamiliar with this style,
// see https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html and
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis.
type StartOption interface {
	apply(c *startConfig)
}

// startOptionFunc implements StartOption interface.
type startOptionFunc func(c *startConfig)

// apply the configuration to the provided config.
func (fn startOptionFunc) apply(c *startConfig) {
	fn(c)
}

// OptionFunc implements Option interface.
type optionFunc func(c *config)

// apply the configuration to the provided config.
func (fn optionFunc) apply(c *config) {
	fn(c)
}

// WithLinearizableReadSafe guarantees the linearizability of the read request by
// communicating with the quorum. It is the default and suggested option.
func WithLinearizableReadSafe() Option {
	return optionFunc(func(c *config) {
		// no-op.
	})
}

// WithLinearizableReadLeaseBased ensures linearizability of the read only request by
// relying on the leader lease. It can be affected by clock drift.
// If the clock drift is unbounded, leader might keep the lease longer than it
// should (clock can move backward/pause without any bound). ReadIndex is not safe
// in that case.
func WithLinearizableReadLeaseBased() Option {
	return optionFunc(func(c *config) {
		c.rcfg.ReadOnlyOption = raft.ReadOnlyLeaseBased
	})
}

// WithTickInterval is the time interval to,
// increments the internal logical clock for,
// the current raft member by a single tick.
//
// Default Value: 100'ms.
func WithTickInterval(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.tickInterval = d
	})
}

// WithStreamTimeOut is the timeout on the streaming messages to other raft members.
//
// Default Value: 10's.
func WithStreamTimeOut(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.streamTimeOut = d
	})
}

// WithDrainTimeOut is the timeout on the streaming pending messages to other raft members.
// Drain can be very useful for graceful shutdown.
//
// Default Value: 10's.
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

// WithMaxSnapshotFiles is the number of snapshots to keep beyond the
// current snapshot.
//
// Default Value: 5.
func WithMaxSnapshotFiles(max int) Option {
	return optionFunc(func(c *config) {
		c.maxSnapshotFiles = max
	})
}

// WithSnapshotInterval is the number of log entries between snapshots.
//
// Default Value: 1000.
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

// WithHeartbeatTick is the number of node tick (WithTickInterval) invocations that
//
//	must pass between heartbeats. That is, a leader sends heartbeat messages to
//
// maintain its leadership every HeartbeatTick ticks.
//
// Default Value: 1.
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

// WithMaxCommittedSizePerReady limits the size of the committed entries which
// can be applied.
//
// Default Value: 0.
func WithMaxCommittedSizePerReady(max uint64) Option {
	return optionFunc(func(c *config) {
		c.rcfg.MaxCommittedSizePerReady = max
	})
}

// WithMaxUncommittedEntriesSize limits the aggregate byte size of the
// uncommitted entries that may be appended to a leader's log. Once this
// limit is exceeded, proposals will begin to return ErrProposalDropped
// errors. Note: 0 for no limit.
//
// Default Value: 1 << 30.
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

// WithPreVote enables the Pre-Vote algorithm described in raft thesis section
// 9.6. This prevents disruption when a node that has been partitioned away
// rejoins the cluster.
//
// Default Value: false.
func WithPreVote() Option {
	return optionFunc(func(c *config) {
		c.rcfg.PreVote = true
	})
}

// WithDisableProposalForwarding set to true means that followers will drop
// proposals, rather than forwarding them to the leader. One use case for
// this feature would be in a situation where the Raft leader is used to
// compute the data of a proposal, for example, adding a timestamp from a
// hybrid logical clock to data in a monotonically increasing way. Forwarding
// should be disabled to prevent a follower with an inaccurate hybrid
// logical clock from assigning the timestamp and then forwarding the data
// to the leader.
//
// Default Value: false.
func WithDisableProposalForwarding() Option {
	return optionFunc(func(c *config) {
		c.rcfg.DisableProposalForwarding = true
	})
}

// WithContext set raft node parent ctx, The provided ctx must be non-nil.
//
// The context controls the entire lifetime of the raft node:
// obtaining a connection, sending the msgs, reading the response, and process msgs.
//
// Default Value: context.Background().
func WithContext(ctx context.Context) Option {
	return optionFunc(func(c *config) {
		c.ctx = ctx
	})
}

// WithLogger sets logger that is used to generates lines of output.
//
// Default Value: raftlog.DefaultLogger.
func WithLogger(lg raftlog.Logger) Option {
	return optionFunc(func(c *config) {
		c.logger = lg
	})
}

// WithPipelining is the process to send successive requests,
// over the same persistent connection, without waiting for the answer.
// This avoids latency of the connection. Theoretically,
// performance could also be improved if two or more requests were to be packed into the same connection.
//
// Note: pipelining spawn 4 goroutines per remote member connection.
func WithPipelining() Option {
	return optionFunc(func(c *config) {
		c.pipelining = true
	})
}

// WithJoin send rpc request to join an existing cluster.
func WithJoin(addr string, timeout time.Duration) StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.Join(addr, timeout)
		c.appendOperator(opr)
	})
}

// WithForceJoin send rpc request to join an existing cluster even if already part of a cluster.
func WithForceJoin(addr string, timeout time.Duration) StartOption {
	return startOptionFunc(func(c *startConfig) {
		opr := raftengine.ForceJoin(addr, timeout)
		c.appendOperator(opr)
	})
}

// WithInitCluster initialize a new cluster and create first raft node.
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
		// create new startConfig annd apply all opts,
		// then copy all operators to fallback.
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
