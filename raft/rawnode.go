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
	return (left.Vote == right.Vote && left.Term == right.Term && left.Commit == right.Commit)
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft         *Raft
	preHardState pb.HardState
}

type ReadyState struct {
	HardState      pb.HardState
	UnstableEmpty  bool
	LastApplyIndex uint64
	LastApplyTerm  uint64
	MsgsLen        uint64
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	node := &RawNode{
		Raft:         raft,
		preHardState: raft.hardState(),
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
	log.Debugf("raft_id: %v, 2B=>  rawnode propose data data len %v", rn.Raft.id, len(data))
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

//  ApplyConfChange applies a config change to the local node.
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
//
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	// 有未发的信息
	// 有未 stable 的 entries
	// 有未 apply 的 entries
	// hardState change
	// log.Debugf("%v %v %v",ShowHardState(rn.Raft.hardState()),ShowHardState(rn.preHardState),!hardStateEq(rn.Raft.hardState(), rn.preHardState))
	hardStateChange := !hardStateEq(rn.Raft.hardState(), rn.preHardState)
	unSendMsgs := len(rn.Raft.msgs) != 0
	unStableEntries := len(rn.Raft.RaftLog.entries) != 0
	unApplyEntries := rn.Raft.RaftLog.applied != rn.Raft.RaftLog.committed
	// log.Debugf("hardStateChange %v unSendMsgs %v unStableEntries %v unApplyEntries %v", hardStateChange, unSendMsgs, unStableEntries, unApplyEntries)
	return hardStateChange || unSendMsgs || unStableEntries || unApplyEntries
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {

	ready := Ready{}
	ready.UnApplyEntry = []pb.Entry{}
	ready.UnStableEntry = []pb.Entry{}

	curHardState := rn.Raft.hardState()

	unapplyEntries := rn.Raft.RaftLog.unAppliedEntis()
	log.Debugf("raft_id :%v  ===>  ready unapply len %v commit  %v  apply  %v ents %v state %v", rn.Raft.id, len(unapplyEntries), rn.Raft.RaftLog.committed, rn.Raft.RaftLog.applied, ShowEntries(unapplyEntries), ShowHardState(curHardState))
	if len(unapplyEntries) != int(rn.Raft.RaftLog.committed-rn.Raft.RaftLog.applied) {
		panic("xxx len ???")
	}

	unstableEntries := rn.Raft.RaftLog.unstableEntries()
	msgs := rn.Raft.msgs
	ready.HardState = curHardState

	ready.UnApplyEntry = unapplyEntries
	ready.UnStableEntry = unstableEntries
	ready.Messages = msgs
	return ready
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// TODO 这个rd 应当与pendingReady是相等的
	rn.preHardState = rd.HardState
	log.Debugf("raft_id: %v, in advace", rn.Raft.id)
	if len(rd.UnStableEntry) > 0 {
		e := rd.UnStableEntry[len(rd.UnStableEntry)-1]
		log.Debugf("raft_id: %v, tag:commit-apply advance update stable %v", rn.Raft.id, e.Index)
		rn.Raft.RaftLog.stabled = e.Index
	}

	if len(rd.UnApplyEntry) > 0 {
		e := rd.UnApplyEntry[len(rd.UnApplyEntry)-1]
		log.Debugf("raft_id: %v, tag:commit-apply  log 2B=> advance update apply %v", rn.Raft.id, e.Index)
		if e.Index > rn.Raft.RaftLog.committed {
			log.Errorf("raft_id: %v, apply %v commit %v", rn.Raft.id, e.Index, rn.Raft.RaftLog.committed)
			panic("advance apply > commit????")
		}
		rn.Raft.RaftLog.applied = e.Index
	}

	rn.Raft.msgs = []pb.Message{}
	rn.Raft.RaftLog.clearEntries()
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
