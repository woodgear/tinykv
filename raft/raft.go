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
	_ "fmt"
	"math/rand"
	"sort"
	"time"

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
	// commit id?
	Applied uint64

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1].
	randomizedElectionTimeout int
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
// Match 记录的是follower的commit的index
// leader 会将Next->lastIndex发给follower 通过递减Next来找到follower能接受的log位置
// 在初始时 leader不知道follower的Match(commit) 默认值为0, Next设置为当前的LastIndex,
// 当follower的 AppendResponse返回时会告诉Leader自己的commit,这是server就能重新设置Match了
// 所以在正常情况下Next = Match+1
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	// who you vote for
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
	// election interval reset each candidate state
	electionTimeout int
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

	pendingSnapShot map[uint64]bool
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex          uint64
	randomizedElectionTimeout int
}

func ShowPrs(prs map[uint64]*Progress) string {
	msg := ""
	for id, p := range prs {
		msg = fmt.Sprintf("%s id %v progress %v %v", msg, id, p.Match, p.Next)
	}
	return msg
}

func random(start int, end int) int {
	rand.Seed(time.Now().UnixNano())
	return start + rand.Intn(end-start)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raft := new(Raft)
	// 从之前的config中我们可以获取到的信息
	hardState, conf, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raft.Term = hardState.Term
	raft.Vote = hardState.Vote

	raft.electionTimeout = c.ElectionTick
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.randomizedElectionTimeout = random(raft.electionTimeout, 2*raft.electionTimeout)

	raft.RaftLog = newLog(c.Storage)
	//TODO ????
	raft.RaftLog.committed = hardState.Commit

	log.Debugf("raft_id: %v, newRaft log %v", c.ID, raft.RaftLog)

	raft.State = StateFollower
	raft.votes = map[uint64]bool{}
	raft.id = c.ID
	raft.pendingSnapShot = make(map[uint64]bool)
	raft.Prs = make(map[uint64]*Progress)
	// 从 confstate中恢复
	if len(c.peers) == 0 && len(conf.Nodes) != 0 {
		for _, prsID := range conf.Nodes {
			raft.Prs[prsID] = &Progress{}
		}
	} else {
		for _, prsID := range c.peers {
			raft.Prs[prsID] = &Progress{}
		}
	}

	return raft
}

func (r *Raft) getElectionTimeout() int {
	return r.randomizedElectionTimeout
}

func (r *Raft) resetElectionElpased() {
	r.electionElapsed = 0
}

func (r *Raft) resetHearbeat() {
	r.heartbeatElapsed = 0
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateCandidate, StateFollower:
		{
			if r.electionElapsed+1 >= r.randomizedElectionTimeout {
				log.Debugf("raft_id: %v, tag: GenericTest log: timeout %v start election", r.id, r.randomizedElectionTimeout)
				r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
			} else {
				r.electionElapsed++
			}
		}

	case StateLeader:
		{
			if r.heartbeatElapsed+1 >= r.heartbeatTimeout {
				log.Debugf("leader start heartbeat")
				r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
			} else {
				r.heartbeatElapsed++
			}
			r.checkPendingSnapshot()
		}
	}
}

func (r *Raft) checkPendingSnapshot() {
	for id, needSendSnap := range r.pendingSnapShot {
		if needSendSnap {
			log.Infof("raft_id: %v,tag: snapshot,log: find a pendingSnapshot %v\n", r.id, id)
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// step 方法中主要处理异常情况 和调用正常的处理逻辑
// 命名规范 xxxHandlexxx 表明只有处于这个state才能处理这个msg 函数内不会对自己的state 做校验 外部做校验
// 		   handleXXX    理论上说所有的state都可以处理这个msg 函数内自己处理
func (r *Raft) Step(m pb.Message) error {
	log.Debugf("raft_id: %v, step %v state %v from %v term %v im %v term %v", r.id, m.MsgType, r.State, m.From, m.Term, r.id, r.Term)
	// 首先处理一些内部消息
	if m.MsgType == pb.MessageType_MsgBeat && r.State == StateLeader {
		r.leaderHandleMsgBeat()
		return nil
	}

	if m.MsgType == pb.MessageType_MsgSnapshot {
		r.handleSnapshot(m)
		return nil
	}

	// follower -> candidate  candidate -> candidate
	if m.MsgType == pb.MessageType_MsgHup && (r.State == StateFollower || r.State == StateCandidate) {
		r.handleMsgHup()
		return nil
	}

	// 如果收到比自己大的term直接变为followe状态
	// 通过这次term的再次选举来恢复状态
	// 假设有一个网络故障的candidiate
	// 虽然大家都变成了follower
	// 但因为有 preTerm 和 preLog 的校验 大家拒绝非法的 candidate当leader所以所人term会飙升 但不会破坏安全性 所以没什么问题
	// 要注意的是这里是> 不是>= 因为AppendEntriesResponse和 RequestVoteResponse中的就是 =发送者的Term的
	// TODO is that a goold choice??
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// 检查 request vote 的 lastLogIndex 和 lastLogTerm 将那些非法的candidate 直接拒绝掉
	if m.MsgType == pb.MessageType_MsgRequestVote {
		msg := new(RequestVoteRequest)
		msg.FromPb(m)

		// TODO why this function
		isValidCandidate := func(followerLastIndex uint64, followerLastIndexTerm uint64, msg *RequestVoteRequest) bool {
			isValid := msg.LastLogTerm > followerLastIndexTerm || (msg.LastLogTerm == followerLastIndexTerm && msg.LastLogIndex >= followerLastIndex)
			log.Debugf("raft_id: %v,  ,tag: election, log check candidate valid %v can-log %v can-term %v cur-log %v cur-term %v", r.id, isValid, msg.LastLogIndex, msg.LastLogTerm, followerLastIndex, followerLastIndexTerm)
			return isValid
		}

		lastIndex := r.GetLastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			return nil
		}

		// 在投票前 我们要先将不合格的candidate过滤掉
		if !isValidCandidate(lastIndex, lastTerm, msg) {
			log.Debugf("raft_id: %v, tag: election, log: reject cause of inconsist log and term from %v im %v", r.id, m.From, r.id)
			r.sendRejectVote(msg.CandidateId)
			return nil
		}
		// 在这里才是正常的follower 处理request vote的逻辑
		return r.followerHandleRequestVote(msg)
	}

	// 在赢得大多数选票时 candidate就成了leader 此时还会收到voteresponse 这时的voteresponse 就没有意义了 忽视即可
	if m.MsgType == pb.MessageType_MsgRequestVoteResponse && r.Term == m.Term && r.State == StateCandidate {
		msg := new(RequestVoteResponse)
		msg.FromPb(m)
		r.candidateHandleRequestVoteResponse(msg)
	}

	if m.MsgType == pb.MessageType_MsgAppend {
		// 当收到一个同级的msg append时可能是当选的leader发过来的 所以直接变为follower 并且设置leader
		if m.Term == r.Term && r.Lead != m.From {
			log.Debugf("raft_id: %v, term == me and im not folllowe im %v my id %v his id %v term %v handle append msg", r.id, r.State, r.id, m.From, r.Term)
			r.becomeFollower(m.Term, m.From)
		}
		// 理论上说leader不可能收到一个同级term的append
		log.Debugf("raft_id: %v, my id %v term %v his id %v term %v handle append msg", r.id, r.id, r.Term, m.From, m.Term)
		if r.State == StateFollower {
			r.handleAppendEntries(m)
		}
	}

	if m.MsgType == pb.MessageType_MsgHeartbeat && r.State == StateFollower && m.Term == r.Term && r.Lead == m.From {
		r.handleHeartbeat(m)
		return nil
	}

	if m.MsgType == pb.MessageType_MsgPropose {
		if r.State == StateFollower {
			// r.redirect(m)
			return nil
		}
		if r.State == StateLeader {
			r.leaderHandleMsgPropose(m)
		}
	}

	if m.MsgType == pb.MessageType_MsgAppendResponse {
		if r.State != StateLeader {
			return nil
		}
		if r.Term != m.Term {
			return nil
		}
		return r.leaderHandleMsgAppendResponse(m)
	}

	return nil
}

func (r *Raft) leaderHandleMsgBeat() {
	r.heartbeatElapsed = 0

	r.tryCommit()
	r.sendHeartbeatToAll()
}

func (r *Raft) handleMsgHup() {
	r.becomeCandidate()
	if len(r.Prs) <= 1 {
		r.winTheElection()
	} else {
		r.Vote = r.id
		r.votes[r.id] = true
		r.sendVoteToAll()
	}
}

func (r *Raft) sendAppendToAll() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// TODO 优化1. 发送空的entries直到匹配正确 优化2. 使用二分查找来匹配
// TODO return true if a message was sent ???
// 当entries被compact且snapshoot没有生成时 直接return false
func (r *Raft) sendAppend(to uint64) bool {
	//TODO check this logic
	// if r.RaftLog.IsEmpty() {
	// 	log.Infof("raft_id: %v,tag: leader-view ,log: raftlog is empty \n", r.id)
	// 	return false
	// }

	next := r.Prs[to].Next
	lastIndex := r.GetLastIndex()
	if next > lastIndex {
		log.Debugf("raft_id: %v,tag: leader-view ,log: empty append??? maybe to update commit", r.id)
	}
	// could not find entry
	// TODO some werid
	// RaftLog里面什么都没有 什么都不用发
	if next-1 < r.RaftLog.initIndex {
		log.Warnf("raft_id: %v,tag: leader-view ,log: initIndex > next??")
		return false
	}
	log.Infof("raft_id: %v,tag: leader-view ,log: entries %v %v sendAppend to %v\n", r.id, next, lastIndex+1, to)
	entries, err := r.RaftLog.PtrEntries(next, lastIndex+1)
	if err == ErrCompacted {
		log.Infof("raft_id: %+v,tag: snapshot leader-view ,log: find compact entries send snapshot to %v\n", r.id, to)
		snapshot, err := r.RaftLog.Snapshot()
		if err == ErrSnapshotTemporarilyUnavailable {
			r.pendingSnapShot[to] = true

			log.Infof("raft_id: %+v,tag: leader-view ,log: snapshot unabiable %v\n", r.id, to)
			return false
		}
		log.Infof("raft_id: %+v,tag: snapshot leader-view ,log: snapshot ok send it\n", r.id)
		r.pendingSnapShot[to] = false
		r.sendSnapshot(to, snapshot)
		return true
	}

	if err != nil {
		panic(err)
	}
	preLogIndex := next - 1

	preLogTerm, error := r.RaftLog.Term(preLogIndex)
	if error != nil {
		panic(error)
	}
	log.Debugf("raft_id: %v, sendAppend to %v lastIndex %v next %v preLogIndex %v preLoogTerm %v", r.id, to, lastIndex, next, preLogIndex, preLogTerm)
	r.sendAppendToFollower(AppendEntriesRequest{
		Term:         r.Term,
		LeaderID:     r.id,
		To:           to,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: r.getCommitedID(),
	})
	return true
}

func (r *Raft) sendSnapshot(to uint64, snap pb.Snapshot) {
	m := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Commit:   r.getCommitedID(),
		Snapshot: &snap,
	}
	r.send(m)
}

// TODO why return error
func (r *Raft) followerHandleMsgAppend(m *AppendEntriesRequest) error {
	leaderCommit := m.LeaderCommit
	leaderID := m.LeaderID
	entries := m.Entries
	preLogIndex := m.PrevLogIndex
	preLogTerm := m.PrevLogTerm
	if len(entries) > 1 {
		log.Debugf("raft_id: %v, followerHandleMsgAppend entries >1", r.id)
	}
	log.Debugf("raft_id: %v, tag:GenericTest , log: im follower leader is %v handle append entry preLogIndex %v preLogTerm %v entries len %v entries %s", r.id, r.Lead, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), ShowPtrEntries(entries))
	//TODO ? check error type
	lastMatchIndex, err := r.RaftLog.tryAppendEntries(preLogIndex, preLogTerm, entries)
	if err != nil {
		log.Debugf("raft_id: %v,tag: time, log: i'm follower, i reject server entries  my lastindex is %v err is %v", r.id, r.GetLastIndex(), err)
		r.rejectAppenEntries(leaderID, r.GetLastIndex())
		return nil
	}

	log.Debugf("raft_id: %v, follower append entry accept %v", r.id, r.GetLastIndex())
	r.acceptAppenEntries(leaderID, lastMatchIndex)
	//leader发来的entries的lastIndex和eladerCommit并不一定是相同的
	lastIndex := r.GetLastIndex()
	log.Debugf("raft_id: %v, leader is %vleader commit %v cur commit %v lastIndex %v  lastMatchupIndex %v", r.id, leaderID, leaderCommit, r.getCommitedID(), lastIndex, lastMatchIndex)

	// TestHandleMessageType_MsgAppend2AB
	shouldCommit := min(leaderCommit, lastMatchIndex)
	shouldCommit = max(shouldCommit, r.getCommitedID())
	if shouldCommit > r.getCommitedID() {
		r.commit(shouldCommit)
	}
	return nil
}

func (r *Raft) leaderHandleMsgAppendResponse(m pb.Message) error {
	if m.Reject {
		// TODO better
		if r.Prs[m.From].Next > m.Index && m.Index != 0 {
			r.Prs[m.From].Next = m.Index
		} else {
			// TODO werid
			if r.Prs[m.From].Next != 0 {
				r.Prs[m.From].Next--
			}
		}
		log.Debugf("raft_id: %v, tag:GenericTest,leader-view log: handleAppendResponse append reject follower is %v follower lastindex is %v next %v", r.id, m.From, m.Index, r.Prs[m.From].Next)

		r.sendAppend(m.From)
	} else {
		logTerm, err := r.RaftLog.Term(m.Index)
		log.Debugf("logTerm %+v %v %v", logTerm, r.Term, m.Index)
		if err != nil {
			return err
		}
		// TODO some werid
		if logTerm != r.Term {
			log.Debugf("leader only commit current term log")
			return nil
		}

		log.Debugf("raft_id: %v tag: leader-view, log: handleAppendResponse append accept per id %v index %v", r.id, m.From, m.Index)
		// 正常情况
		// 更新对应follower的progress
		// Match代表follower和leader的最长公共前缀序列
		// 所以Next就是Match+1
		// 这样下次发送prelog 就是Match 发送 Match+1->LastIndex 到follower那就不会有任何冲突
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1

		couldCommit, commitID := r.checkCommit()
		log.Debugf("couldCommit %v commitID %v", couldCommit, commitID)
		if couldCommit {
			r.commit(commitID)
			r.sendAppendToAll()
		}
	}
	return nil
}

/// commit 如果可以的话
func (r *Raft) tryCommit() {
	couldCommit, commitID := r.checkCommit()
	log.Debugf("raft_id: %v, tryCommit could commit %v commit %v cur commit %v", r.id, couldCommit, commitID, r.getCommitedID())
	if couldCommit {
		r.commit(commitID)
	}
}

func (r *Raft) leaderHandleMsgPropose(m pb.Message) {
	// 1. 将entry存到自己的log中
	// client request entries only contains data
	r.RaftLog.appendClientEntries(m.Entries, r.Term)
	log.Debugf("raft_id: %v,tag: leader-view, log: leader handle msg propose entries len %v lastindex %v", r.id, len(m.Entries), r.GetLastIndex())

	log.Debugf("raft_id: %v, after leader handle propose len %v lastIndex %v commit %v", r.id, len(m.Entries), r.GetLastIndex(), r.getCommitedID())
	// 2. 更新自己的状态
	r.Prs[r.id].Match = r.GetLastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	r.tryCommit()
	// 3. 向自己的follower发append请求
	r.sendAppendToAll()
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) getTerm(index uint64) uint64 {
	term, error := r.RaftLog.Term(index)
	if error != nil {
		panic(error)
	}
	return term
}

func (r *Raft) winTheElection() {
	r.becomeLeader()
	r.tryCommit()
	// TODO 奇怪的操作
	r.sendHeartbeatToAll()
	r.sendAppendToAll()
}

func (r *Raft) candidateHandleRequestVoteResponse(m *RequestVoteResponse) {
	type VoteType uint64
	const (
		VoteWin VoteType = iota
		VoteLose
		VoteNotYet
	)
	amIwin := func(r *Raft) VoteType {
		var voteMeCount uint64 = 0
		var rejectMeCount uint64 = 0
		for _, voteMe := range r.votes {
			if voteMe {
				voteMeCount++
			} else {
				rejectMeCount++
			}
		}

		// voteMeCount, rejectMeCount
		halfCount := uint64(len(r.Prs) / 2)
		log.Debugf("raft_id: %v tag: election log: am i win vote me %v rejectMe %v len %v ", r.id, voteMeCount, rejectMeCount, len(r.Prs))
		if voteMeCount > halfCount {
			return VoteWin
		} else if rejectMeCount > halfCount {
			return VoteLose
		} else {
			return VoteNotYet
		}
	}

	r.votes[m.From] = m.VoteGranted
	switch amIwin(r) {
	case VoteWin:
		log.Debugf("raft_id: %v,  ,tag:election,log: win the election", r.id)
		r.winTheElection()
	case VoteLose:
		log.Debugf("raft_id: %v,  ,tag: election ,log: election lose", r.id)
		r.becomeFollower(r.Term, r.Lead)
	case VoteNotYet:
		log.Debugf("raft_id: %v, tag: election, log: election not yet", r.id)
	}
}

func (r *Raft) followerHandleRequestVote(m *RequestVoteRequest) error {
	if r.Vote == None {
		r.Vote = m.CandidateId
		log.Debugf("raft_id: %v, tag:election log: vote agree candidate-id %v  because term == curTerm and vote is none", r.id, m.CandidateId)
		r.sendAgreeVote(m.CandidateId)
	} else if r.Vote == m.CandidateId {
		log.Debugf("raft_id: %v, tag:election vote agree candidate-id %v because candiate term == curTerm and i have vote you already", r.id, m.CandidateId)
		r.sendAgreeVote(m.CandidateId)
	} else {
		log.Debugf("raft_id: %v, tag: election vote reject %v->%v because candiate term == curTerm and i have vote %v", r.id, r.id, m.CandidateId, r.id)
		r.sendRejectVote(m.CandidateId)
	}
	return nil
}

func (r *Raft) checkCommit() (bool, uint64) {
	matchedIndex := []uint64{}
	for _, p := range r.Prs {
		matchedIndex = append(matchedIndex, p.Match)
	}
	sort.Slice(matchedIndex, func(i, j int) bool { return matchedIndex[i] < matchedIndex[j] })
	halfCount := uint64((len(r.Prs)+1)/2) - 1
	log.Debugf("raft_id: %v, check commit matchedIndex %v prs %+v half count %v total count %v", r.id, matchedIndex, ShowPrs(r.Prs), halfCount, len(r.Prs))

	if matchedIndex[halfCount] > r.getCommitedID() {
		return true, matchedIndex[halfCount]
	}
	return false, None
}

func (r *Raft) getCommitedID() uint64 {
	return r.RaftLog.committed
}

func (r *Raft) getStableID() uint64 {
	return r.RaftLog.stabled
}

func (r *Raft) commit(index uint64) {
	if index > r.GetLastIndex() {
		panic("?? commit bigger that lastindex?")
	}
	log.Debugf("raft_id: %v, tag: commit-apply  log: commit to %v", r.id, index)
	r.RaftLog.commit(index)
}

func (r *Raft) becomeFollower(term uint64, leader uint64) {
	log.Debugf("raft_id: %v, becomeFollower term %v leader %v", r.id, term, leader)
	r.electionElapsed = 0
	r.Vote = 0
	r.Term = term
	r.heartbeatElapsed = 0
	r.Lead = leader
	r.votes = map[uint64]bool{}
	r.State = StateFollower
}

func (r *Raft) becomeCandidate() {
	log.Debugf("raft_id: %v,tag: election, log: becomeCandidate Term %v -> %v", r.id, r.Term, r.Term+1)
	r.Term++
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomizedElectionTimeout = random(r.electionTimeout, 2*r.electionTimeout)
	r.State = StateCandidate
}

func (r *Raft) becomeLeader() {
	log.Debugf("raft_id: %v,  ,tag: election log: 2B=> becomeLeader append entry lastindex %v term %v", r.id, r.GetLastIndex(), r.Term)

	r.State = StateLeader
	// append a empty entries to help commit
	_ = r.RaftLog.appendClientEntries([]*pb.Entry{new(pb.Entry)}, r.Term)
	log.Debugf("raft_id: %v, becomeLeader after appendClientEntries log %v", r.id, r.RaftLog)
	for id := range r.Prs {
		// TestLeaderStartReplication2AB
		// TestBcastBeat2B
		// 这样PreLog就是LastIndex-1 entries 就是[LastIndex]
		r.Prs[id] = &Progress{0, r.GetLastIndex()}
	}
	// TODO why ???? why could not commit pre leader
	//TODO 直接在这里step一个msg?
	r.Prs[r.id].Match = r.GetLastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

func (r *Raft) sendVoteToAll() {
	for id := range r.Prs {
		if id != r.id {
			r.sendVoteRequest(id)
		}
	}
}

func (r *Raft) sendHeartbeatToAll() {
	// 发送心跳给follower
	r.heartbeatElapsed = 0
	for id := range r.Prs {
		if id != r.id {
			// 不能直接将leader的commit发送给follower,要确保follower和leader的log一致之后才能发
			// 所以这里是Match和commit的最小值
			commit := min(r.Prs[id].Match, r.getCommitedID())
			r.sendHeartbeat(r.id, id, r.Term, commit)
		}
	}
}

func (r *Raft) sendAppendToFollower(msg AppendEntriesRequest) {
	m := msg.ToPb()
	log.Debugf("raft_id: %v, sendAppendToFollower %v %+v", r.id, m.To, msg)
	r.send(m)
}

func (r *Raft) GetLastIndex() uint64 {
	return r.RaftLog.LastIndex()
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(from uint64, to uint64, term uint64, commit uint64) {
	log.Debugf("raft_id: %v, heartbeat to %v term %v commit %v", from, to, term, commit)
	msg := pb.Message{From: from, To: to, Term: term, Commit: commit, MsgType: pb.MessageType_MsgHeartbeat}
	r.send(msg)
}

func (r *Raft) sendRejectVote(to uint64) {
	msg := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
	r.send(msg)
}

func (r *Raft) sendAgreeVote(to uint64) {
	msg := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false}
	r.send(msg)
}

func (r *Raft) sendVoteRequest(id uint64) {
	lastLogIndex := r.GetLastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)
	//TODO fix this
	if err != nil {
		panic(err)
	}

	log.Debugf("raft_id: %v,  ,tag: election log: sendVoteRequest to %v lastIndex %v lastTerm %v", r.id, id, lastLogIndex, lastLogTerm)
	r.send(pb.Message{From: r.id, To: id, Term: r.Term, Index: lastLogIndex, LogTerm: lastLogTerm, MsgType: pb.MessageType_MsgRequestVote})
}

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// handleAppendEntries handle AppendEntries RPC request
// followerHandleAppendEntries
func (r *Raft) handleAppendEntries(m pb.Message) {
	msg := new(AppendEntriesRequest)
	msg.FromPb(m)
	r.followerHandleMsgAppend(msg)
}

func (r *Raft) acceptAppenEntries(id uint64, lastMatchIndex uint64) {
	r.send(pb.Message{From: r.id, To: id, Term: r.Term, Index: lastMatchIndex, Reject: false, MsgType: pb.MessageType_MsgAppendResponse})
}

func (r *Raft) rejectAppenEntries(id uint64, lastIndex uint64) {
	r.send(pb.Message{From: r.id, To: id, Term: r.Term, Index: lastIndex, Reject: true, MsgType: pb.MessageType_MsgAppendResponse})
}

func (r *Raft) apply(id uint64) {
	r.RaftLog.applied = id
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	if m.Commit > r.getCommitedID() {
		commit := min(m.Commit, r.GetLastIndex())
		log.Debugf("raft_id: %v, heartbeat commit %v cur commit %v", r.id, commit, r.getCommitedID())
		r.commit(commit)
	}
	r.sendHeartbeatResponse(m.From)
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	r.send(pb.Message{From: r.id, To: to, Term: r.Term, Index: r.GetLastIndex(), MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	log.Infof("raft_id: %v,tag: snapshot,log: handlesnapshot snapindex %v lastindex %v\n", r.id, m.Snapshot.Metadata.Index, r.RaftLog.LastIndex())
	if m.Snapshot.Metadata.Index > r.RaftLog.LastIndex() {
		r.Lead = m.From
		r.updateNodes(m.Snapshot.Metadata.ConfState.Nodes)
		r.RaftLog.handleSnapshot(m.Snapshot)
		r.acceptAppenEntries(m.From, m.Snapshot.Metadata.Index)
	}
}

func (r *Raft) updateNodes(nodes []uint64) {
	for _, prsID := range nodes {
		r.Prs[prsID] = &Progress{}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
