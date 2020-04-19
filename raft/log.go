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
	log.Debugf("init log  sfirst %v slast %v offset %v last %v initIndex %v initTerm %v len %v", firstIndex, lastIndex, l.offset, l.LastIndex(), initIndex, initTerm, len(l.entries))
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

func (l *RaftLog) getEntry(index uint64) *pb.Entry {
	//TODO ????????
	if index == 0 {
		return &pb.Entry{}
	}
	log.Debugf("get entry index %v offset %v real %v", index, l.offset, index-l.offset)
	return &l.entries[index-l.offset]
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) status() {
	fmt.Printf("call status apply %v commit %v stable %v lastindex %v entry len %v\n", l.applied, l.committed, l.stabled, l.LastIndex(), len(l.entries))
	for _, e := range l.entries {
		fmt.Printf("term %v index %v data %v\n", e.Term, e.Index, e.Data)
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	entries := []pb.Entry{}

	for index := l.stabled + 1; index <= l.LastIndex(); index++ {
		entry := l.getEntry(index)
		if entry == nil {
			panic("err find a nil entry")
		}
		entries = append(entries, *entry)
	}
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.unAppliedEntis()
}

func (l *RaftLog) unAppliedEntis() (ents []pb.Entry) {
	entries := []pb.Entry{}
	for index := l.applied + 1; index <= l.committed; index++ {
		entry := l.getEntry(index)
		if entry == nil {
			panic("err find a nil entry")
		}
		entries = append(entries, *entry)
	}
	return entries
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == l.initIndex {
		return l.initTerm, nil
	}
	entry := l.getEntry(i)
	return entry.Term, nil

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

	if preLogIndex > l.LastIndex() || l.getEntry(preLogIndex).Term != preLogTerm {
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
		fmt.Printf("tryAppendEntries: conflict false log = flog\n")
	} else if hasConflict == false && serverEntriesLastIndex > l.LastIndex() {
		// 2. serverlog与follower entries不冲突 且存在交集
		fmt.Printf("tryAppendEntries: conflict false log = slog + part of flog\n")
		l.entries = append(l.entries, serverEntries[serverSamedIndex+1:]...)
	} else {
		// 3. entries冲突
		// 这时我们要重置stable,因为stable有可能也被server的log改变了
		fmt.Printf("tryAppendEntries: conflict true log = part of slog + flog\n")
		l.entries = append(l.entries[:followerSamedIndex+1], serverEntries...)
		l.stabled = min(uint64(followerSamedIndex)+l.offset, l.stabled)
	}
	return l.entries[len(l.entries)-1].Index, nil
}
