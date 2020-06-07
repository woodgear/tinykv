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

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// TODO 一个有趣的问题在于 first applied commited stable 的定义是什么,是否需要+1,当GetEntry（Commit）返回的是最后一个Commit的Entry 还是第一个uncommit的Entry
// 我使用的定义是前者
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// storage中的firstIndex
	offset    uint64
	initIndex uint64
	initTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	l := new(RaftLog)
	l.storage = storage
	l.entries = []pb.Entry{}

	firstIndex, error := storage.FirstIndex()
	if error != nil {
		return nil
	}

	lastIndex, error := storage.LastIndex()
	if error != nil {
		return nil
	}
	initIndex := firstIndex - 1
	initTerm, error := storage.Term(initIndex)
	// TODO
	if error != nil {
		panic(error)
	}
	// 0 0  或者snapshot的最后的Index和Term
	l.initIndex = initIndex
	l.initTerm = initTerm
	l.stabled = lastIndex
	l.applied = initIndex // apply 最起码是snapindex
	// if lastIndex < firstIndex 这表示storage中没有任何 compact->stable之间的entry
	// 说明这是新起的Raft Node
	if lastIndex >= firstIndex {
		//我们希望拿到storage中所有的compact->stable的entries
		// 因为entries是[) 所以lastIndex要+1
		entries, error := storage.Entries(firstIndex, lastIndex+1)
		if error != nil {
			return nil
		}
		l.entries = append(l.entries, entries...)
		l.offset = l.entries[0].Index
	} else {
		l.offset = firstIndex
	}
	return l
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	// if len(l.entries) ==0 we must make sure LastIndex+1 == offset
	// thus that first been apped entry index will be lastIndex+1 which in fact is offset
	if len(l.entries) == 0 {
		return l.offset - 1
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) GetTerm(index uint64) uint64 {
	term, err := l.Term(index)
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) isValidIndex(index uint64) error {
	realIndex := index - l.offset
	if realIndex >= 0 && realIndex < uint64(len(l.entries)) {
		return nil
	}
	return errors.New(fmt.Sprintf("invalid index %v offset %v len %v", index, l.offset, len(l.entries)))
}

// Entries from raftlog if low or hight not in range it will panic
// 将raftlog 视为数组
func (l *RaftLog) Entries(low, hight uint64) []pb.Entry {
	log.Debugf("get entries %v %v", low, hight)
	if low > hight {
		panic(fmt.Errorf("Entries low>hight %v %v fail", low, hight))
	}
	if low == hight {
		return []pb.Entry{}
	}
	if err := l.isValidIndex(low); err != nil {
		panic(err)
	}
	if err := l.isValidIndex(hight - 1); err != nil {
		panic(err)
	}
	entries := []pb.Entry{}
	for index := low; index < hight; index++ {
		entries = append(entries, *l.getEntry(index))
	}
	return entries
}

// PtrEntries same as Entries but return []*pb.Entry instead of []pb.Entry
func (l *RaftLog) PtrEntries(low, hight uint64) []*pb.Entry {
	entries := l.Entries(low, hight)
	ptrEntries := []*pb.Entry{}
	for i, _ := range entries {
		ptrEntries = append(ptrEntries, &entries[i])
	}
	return ptrEntries
}

func (l *RaftLog) getEntry(index uint64) *pb.Entry {
	realIndex := index - l.offset
	return &l.entries[realIndex]
}

// log中是否为空 （初始化时）
func (l *RaftLog) IsEmpty() bool {
	return len(l.entries) == 0
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) String() string {
	status := fmt.Sprintf(`Log { initIndex: %v,initTerm: %v,commit: %v, stable: %v,apply: %v, lastindex: %v,offset: %v,entry_len: %v,entries: %v}`,
		l.initIndex,
		l.initTerm,
		l.committed,
		l.stabled,
		l.applied,
		l.LastIndex(),
		l.offset,
		len(l.entries),
		ShowEntries(l.entries),
	)
	return status
}

func (l *RaftLog) MetaString() string {
	status := fmt.Sprintf(`Log { initIndex: %v,initTerm: %v,commit: %v, stable: %v,apply: %v, lastindex: %v,offset: %v,entry_len: %v}`,
		l.initIndex,
		l.initTerm,
		l.committed,
		l.stabled,
		l.applied,
		l.LastIndex(),
		l.offset,
		len(l.entries),
	)
	return status
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.Entries(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.unAppliedEntis()
}

func (l *RaftLog) unAppliedEntis() (ents []pb.Entry) {
	// all of commited entry include the last commit entry
	return l.Entries(l.applied+1, l.committed+1)
}

// Term return the term of the entry in the given index
// TODO when term will error???
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == l.initIndex {
		return l.initTerm, nil
	}

	if err := l.isValidIndex(i); err != nil {
		return 0, err
	}
	return l.entries[i-l.offset].Term, nil
}

func (l *RaftLog) Append(e pb.Entry) {
	l.entries = append(l.entries, e)
}

func (l *RaftLog) appendClientEntries(entries []*pb.Entry, term uint64) uint64 {
	for _, e := range entries {
		e.Index = l.LastIndex() + 1
		e.Term = term
		l.Append(*e)
	}
	return l.LastIndex()
}

// TODO better impl
func findConflict(followerLog *[]pb.Entry, serverLog *[]pb.Entry, offset, preLogIndex uint64) (bool, uint64, uint64) {
	//prelogIndex 在log.entries中的真正的index
	preLogIndexInEntsIndex := preLogIndex - offset
	i := preLogIndexInEntsIndex + 1 //preLogIndexInEntsIndex 是两者绝对相同部分 下一次比较从preLogIndexInEntsIndex 0 开始
	var j uint64 = 0
	logOffset := preLogIndexInEntsIndex
	var appendEntryOffset uint64 = 0
	findConflict := false

	for i < uint64(len(*followerLog)) && j < uint64(len(*serverLog)) {
		entry := (*followerLog)[i]
		if !(entry.Index == (*serverLog)[j].Index && entry.Term == (*serverLog)[j].Term) {
			findConflict = true
			break
		} else {
			logOffset = i
			appendEntryOffset = j
		}
		i++
		j++
	}
	return findConflict, logOffset, appendEntryOffset
}

// TestFollowerAppendEntries2AB
// TestHandleMessageType_MsgAppend2AB
func (l *RaftLog) tryAppendEntries(preLogIndex uint64, preLogTerm uint64, entries []*pb.Entry) (uint64, error) {
	// 当我们收到server发来的Entries时 有几种情况需要分类讨论
	// 首先保证 server的Entries是合法的

	if preLogIndex > l.LastIndex() || l.GetTerm(preLogIndex) != preLogTerm {
		return 0, fmt.Errorf("could not find this index and term")
	}

	if len(entries) == 0 {
		return preLogIndex, nil
	}

	// TODO ??? we have to do this?
	serverEntries := []pb.Entry{}
	for _, e := range entries {
		serverEntries = append(serverEntries, *e)
	}

	// 1. server发来的Entries正好接到follower Entries的后边
	if preLogIndex == l.LastIndex() {
		l.entries = append(l.entries, serverEntries...)
		return l.entries[len(l.entries)-1].Index, nil
	}
	// 如果不是第一种情况 我们就不得不去检查server发来的Entries与follower本身的Entries是否有冲突
	hasConflict, followerSamedIndex, serverSamedIndex := findConflict(&l.entries, &serverEntries, l.offset, preLogIndex)

	serverEntriesLastIndex := serverEntries[len(serverEntries)-1].Index
	// 2. follower的Entries完全包含了server发过来的Entries
	if hasConflict == false && serverEntriesLastIndex <= l.LastIndex() {
		log.Debugf("tryAppendEntries: conflict false log = flog\n")
	} else if hasConflict == false && serverEntriesLastIndex > l.LastIndex() {
		// 2. serverlog与follower entries不冲突 且存在交集
		log.Debugf("tryAppendEntries: conflict false log = slog + part of flog\n")
		l.entries = append(l.entries, serverEntries[serverSamedIndex+1:]...)
	} else {
		// 3. entries冲突
		// 这时我们要重置stable,因为stable有可能也被server的log改变了
		log.Warnf("tryAppendEntries: conflict true log = part of slog + flog\n")
		l.entries = append(l.entries[:followerSamedIndex+1], serverEntries...)
		l.stabled = min(uint64(followerSamedIndex)+l.offset, l.stabled)
	}
	return l.entries[len(l.entries)-1].Index, nil
}
