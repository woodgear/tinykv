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

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// UnStableEntry specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// TODO rename to UnStableEntry
	UnStableEntry []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// UnApplyEntry specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// TODO rename to UnApplyEntry
	UnApplyEntry []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages   []pb.Message
	ReadyReson string // apply|stable|term|vote|msgs
}

func (r *Ready) GetUnApplyEntry() []pb.Entry {
	return r.UnApplyEntry
}

func (r *Ready) GetUnStableEntry() []pb.Entry {
	return r.UnStableEntry
}

func (r *Ready) String() string {
	status := fmt.Sprintf(`Ready { HardState: %v, UnStableEntry: %v, UnApplyEntry: %v,Reason: %s  Messages: %v }`,
		ShowHardState(r.HardState),
		ShowEntries(r.UnStableEntry),
		ShowEntries(r.UnApplyEntry),
		r.ReadyReson,
		ShowMsgs(r.Messages),
	)

	return status
}

// TODO make it to method of HardState
func hardStateEq(left, right pb.HardState) bool {
	return !(left.Vote == right.Vote && left.Term == right.Term && left.Commit == right.Commit)
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft            *Raft
	LastStableIndex uint64
	preReadyState   ReadyState
	pendingReady    bool
}

type ReadyState struct {
	HardState       pb.HardState
	LastStableIndex uint64
	LastStableTerm  uint64
	LastApplyIndex  uint64
	LastApplyTerm   uint64
	MsgsLen         uint64
}

// TODO better name
func isReadyStateChange(curReady, preReady ReadyState) (bool, string) {
	stableEq := curReady.LastStableIndex == preReady.LastStableIndex && curReady.LastStableTerm == preReady.LastStableTerm
	applyEq := curReady.LastApplyIndex == preReady.LastApplyIndex && curReady.LastApplyTerm == preReady.LastApplyTerm
	msgEq := curReady.MsgsLen == 0 // 当currentReadyState中没有msg时 ReadyState 关于msg就没有改变
	hardStateEq := IsHardStateEqual(curReady.HardState, preReady.HardState)
	status := fmt.Sprintf("stableEq:%v applyEq:%v curReady.msgLen: %v right.msgLen: %v hardstateEq: %v curReady.hardState: %v right.hardState: %s",
		stableEq, applyEq, curReady.MsgsLen, preReady.MsgsLen, hardStateEq,
		ShowHardState(curReady.HardState),
		ShowHardState(preReady.HardState),
	)
	return hardStateEq && stableEq && applyEq && msgEq, status
}

func (rs ReadyState) String() string {
	return fmt.Sprintf("{hardState:%v ,stable:%v %v ,apply: %v %v,msg: %v}", ShowHardState(rs.HardState),
		rs.LastStableIndex,
		rs.LastStableTerm,
		rs.LastApplyIndex,
		rs.LastApplyTerm,
		rs.MsgsLen)
}
func (rs ReadyState) diff(right ReadyState) string {
	return fmt.Sprintf("left %s right %s", rs.String(), right.String())
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	node := &RawNode{
		Raft:            raft,
		LastStableIndex: raft.getStableID(),
		preReadyState:   raft.GetReadyState(),
		pendingReady:    false,
	}
	return node, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	log.Debugf("raft_id: %v 2B=>  rawnode propose data data len %v", rn.Raft.id, len(data))
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// HasReady called when RawNode user need to check if any Ready pending.
// HasReady保证当Ready->Advance的单序 当前一个Ready未Advance时 HasReady永远返回false
// 首先检查是否有pendingReady
// 接着检查新的Ready与preReady是否有变化
// 有 HasReady 返回true 调用Ready 更新Pending Ready 在advacne时 取消Pending Ready 并更新PreReady
// 不允许递归的HasReady
//
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).

	// 如果有一个Pending的Ready 要等到这个Ready Advance 才能 Ready
	// TODO did we really need this?
	if rn.pendingReady {
		log.Debugf("raft_id:%v has a pending ready", rn.Raft.id)
		return false
	}
	currentReadyState := rn.Raft.GetReadyState()
	isEq, status := isReadyStateChange(currentReadyState, rn.preReadyState)
	if !isEq {
		log.Debugf("raft_id: %v has ready diff %s", rn.Raft.id, status)
		return true
	} else {
		// log.Debugf("raft_id: %v log: not ready diff: %s", rn.Raft.id, status)
		log.Debugf("raft_id: %v log: not ready", rn.Raft.id)
	}
	return false
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {

	ready := Ready{}
	ready.UnApplyEntry = []pb.Entry{}
	ready.UnStableEntry = []pb.Entry{}

	curHardState := rn.Raft.hardState()

	unapplyEntries := rn.Raft.RaftLog.unAppliedEntis()
	unstableEntries := rn.Raft.RaftLog.unstableEntries()
	msgs := rn.Raft.msgs
	ready.HardState = curHardState

	ready.UnApplyEntry = unapplyEntries
	ready.UnStableEntry = unstableEntries
	ready.Messages = msgs
	// TODO is there a goog place
	if rn.pendingReady == false {
		rn.pendingReady = true
	}
	return ready
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// TODO 这个rd 应当与pendingReady是相等的
	rn.preReadyState.HardState = rd.HardState
	log.Debugf("raft_id: %v in advace", rn.Raft.id)
	if len(rd.UnStableEntry) > 0 {
		e := rd.UnStableEntry[len(rd.UnStableEntry)-1]
		log.Debugf("raft_id: %v advance update stable %v", rn.Raft.id, e.Index)
		rn.Raft.RaftLog.stabled = e.Index
		rn.preReadyState.LastStableIndex = e.Index
		rn.preReadyState.LastStableTerm = e.Term
	}

	// TODO apply 到底是看hardState中的commit还是看ready中的commitEntries
	if len(rd.UnApplyEntry) > 0 {
		e := rd.UnApplyEntry[len(rd.UnApplyEntry)-1]
		log.Debugf("raft_id: %v 2B=> advance update apply %v", rn.Raft.id, e.Index)
		rn.Raft.RaftLog.applied = e.Index
		rn.preReadyState.LastApplyIndex = e.Index
		rn.preReadyState.LastApplyTerm = e.Term
	}
	newMsgs := rn.Raft.msgs[len(rd.Messages):]
	log.Debugf("raft_id: %v => advance update msg ready-len %v cur-len %v new-len %v", rn.Raft.id, len(rd.Messages), len(rn.Raft.msgs), len(newMsgs))

	// raft 的msgs是单调增 无修改的
	rn.Raft.msgs = newMsgs
	rn.preReadyState.MsgsLen = uint64(len(newMsgs))
	rn.pendingReady = false
}

// GetProgress return the the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
