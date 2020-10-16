// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (pr *Progress) maybeUpdate(n uint64) bool {
	var update bool
	if pr.Match < n {
		pr.Match = n
		update = true
	}
	pr.Next = max(pr.Next, n+1)
	return update
}

// Raft state
type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// election interval is in the range [electionTimeout, electionTimeout * 2).
	// Reset when raft changes its state to candidate or fellower.
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	logger *log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftlog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if len(c.peers) > 0 {
		cs.Nodes = c.peers
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftlog,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		logger:           log.New(),
	}

	r.logger.SetLevel(log.LOG_LEVEL_DEBUG)
	r.makePrs(c.peers)
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		r.RaftLog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for n := range r.Prs {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}
	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.LastTerm())

	return r
}

func (r *Raft) makePrs(nodes []uint64) {
	r.Prs = make(map[uint64]*Progress)
	for _, node := range nodes {
		r.Prs[node] = &Progress{}
	}
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]
	term, errt := r.RaftLog.Term(pr.Next - 1)
	es, erre := r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex()+1)
	if errt != nil || erre != nil {
		return false
	}
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Term:    r.Term,
		LogTerm: term,
		Index:   pr.Next - 1,
		Entries: entsV2P(es),
		Commit:  r.RaftLog.committed,
	}
	r.send(m)
	return true
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) advance(rd Ready) {
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(last.Index, last.Term)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapshotTo(rd.Snapshot.Metadata.Index)
	}
	if applied := rd.appliedCursor(); applied > 0 {
		r.RaftLog.appliedTo(applied)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	})
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.Prs.
func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id != r.id {
			log.Info("sent heartbeat")
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) appendEntry(esp ...*pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range esp {
		esp[i].Term = r.Term
		esp[i].Index = li + 1 + uint64(i)
	}
	li = r.RaftLog.append(entsP2V(esp)...)
	r.Prs[r.id].maybeUpdate(li)
	r.maybeCommit()
}

// v2p converts value array to pointer array
func entsV2P(es []pb.Entry) []*pb.Entry {
	esp := make([]*pb.Entry, 0, len(es))
	for i := range es {
		esp = append(esp, &es[i])
	}
	return esp
}

// p2v converts pointer array to value array
func entsP2V(esp []*pb.Entry) []pb.Entry {
	es := make([]pb.Entry, 0, len(esp))
	for i := range esp {
		es = append(es, *esp[i])
	}
	return es
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybeCommit() bool {
	mci := r.majorN()
	return r.RaftLog.maybeCommit(r.Term, mci)
}

// majorN returns N such that a majority of matchIndex[i] >= N
func (r *Raft) majorN() uint64 {
	matchs := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matchs = append(matchs, pr.Match)
	}
	sort.Sort(uint64Slice(matchs))
	return matchs[(len(r.Prs)-1)/2]
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartBeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
	}
}

func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
	}
}

// reset raft state
func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
		r.Vote = None
		r.votes = map[uint64]bool{}
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomElectionTimeout()
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
	r.logger.Infof("[%x] became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
	r.logger.Infof("[%x] became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	// reset progress
	for id, pr := range r.Prs {
		pr.Match = 0
		pr.Next = r.RaftLog.LastIndex() + 1
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}
	// propose a noop entry
	noopEnt := &pb.Entry{Data: nil}
	r.appendEntry(noopEnt)
	r.logger.Infof("[%x] became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		// convert to follower
		r.logger.Infof("[%x]-[term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType.String(), m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// message from lower term will be ignored
		r.logger.Infof("[%x]-[term: %d] received a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType.String(), m.From, m.Term)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		res := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		// can vote when voted candidate sends repeat request or
		// haven't vote yet and don't think there's a leader
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			res.Reject = false
			r.electionElapsed = 0
			r.Vote = m.From
		}
		r.send(res)
	default:
		var err error
		switch r.State {
		case StateFollower:
			err = r.stepFollower(m)
		case StateLeader:
			err = r.stepLeader(m)
		case StateCandidate:
			err = r.stepCandidate(m)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		// forward propose to leader
		if r.Lead == None {
			r.logger.Infof("[%x] no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.logger.Infof("[%x] no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, !m.Reject)
		r.logger.Infof("[%x] has received %d votes and %d vote rejections", r.id, gr, rj)
		switch res {
		case VoteWon:
			r.becomeLeader()
			r.bcastAppend()
		case VoteLost:
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		return nil
	}

	// All other message types require a progress for m.From (pr).
	pr := r.Prs[m.From]
	if pr == nil {
		r.logger.Debugf("[%x] no progress available for %x", r.id, m.From)
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			r.logger.Debugf("[%x] received rejected MsgAppResp(conflictIndex: %d, conflictTerm: %d) from %x",
				r.id, m.Index, m.LogTerm, m.From)
			pr.Next = r.RaftLog.leaderFastRollBack(m.Index, m.LogTerm)
			r.sendAppend(m.From)
		} else {
			pr.maybeUpdate(m.Index)
			if r.maybeCommit() {
				r.bcastAppend()
			}
		}
	}
	return nil
}

// VoteResult indicates the outcome of a vote.
type VoteResult uint8

const (
	// VotePending indicates waiting for final results.
	VotePending VoteResult = iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

func (r *Raft) poll(id uint64, v bool) (granted int, rejected int, _ VoteResult) {
	r.recordVote(id, v)
	return r.tallyVotes()
}

// recordVote records that the node with the given id voted for this Raft
// instance if v == true (and declined it otherwise).
func (r *Raft) recordVote(id uint64, v bool) {
	_, voted := r.votes[id]
	if !voted {
		r.votes[id] = v
	}
}

// tallyVotes returns the number of granted and rejected Votes, and whether the
// election outcome is known.
func (r *Raft) tallyVotes() (granted int, rejected int, _ VoteResult) {
	for id := range r.Prs {
		v, voted := r.votes[id]
		if voted {
			if v {
				granted++
			} else {
				rejected++
			}
		}
	}

	res := VotePending
	if granted > len(r.Prs)/2 {
		res = VoteWon
	} else if rejected > len(r.Prs)/2 {
		res = VoteLost
	}
	return granted, rejected, res
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) campaign() {
	if r.State == StateLeader {
		// only follower or candidate can campaign
		r.logger.Debugf("[%x] ignoring MsgHup because already leader", r.id)
		return
	}
	r.becomeCandidate()

	if _, _, res := r.poll(r.id, true); res == VoteWon {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		r.becomeLeader()
		return
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		m := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		}
		r.logger.Infof("[%x]-[logterm: %d, index: %d] sent requestVote to %x at term %d",
			r.id, m.LogTerm, m.Index, m.To, m.Term)
		r.send(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	res := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Term:    r.Term,
	}

	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, entsP2V(m.Entries)...); ok {
		res.Index = mlastIndex
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		res.Index, res.LogTerm = r.RaftLog.fellowerFastRollBack(m.Index, m.LogTerm)
		res.Reject = true
	}
	r.send(res)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
