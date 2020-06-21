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
// staorage 和entries共同提供对Raft算法模块的支持
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
		entries, error := storage.Entries(firstIndex, lastIndex+1)
		if error != nil {
			return nil
		}
		l.entries = append(l.entries, entries...)
	} else {
	}

	return l
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		storageLastIndex, err := l.storage.LastIndex()
		// TODO fix panic
		if err != nil {
			panic(err)
		}
		return storageLastIndex
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
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return err
	}
	if index < firstIndex {
		return fmt.Errorf("invalid index index %v firstIndex %v", index, firstIndex)
	}
	lastIndex := l.LastIndex()
	if index > lastIndex {
		return fmt.Errorf("invalid index index %v lastIndex %v", index, lastIndex)
	}
	return nil
}

func (l *RaftLog) GetEntry(index uint64) (*pb.Entry, error) {
	err := l.isValidIndex(index)
	if err != nil {
		return nil, err
	}
	entryPtr := l.getEntryFromRaftLogIfExist(index)
	if entryPtr != nil {
		return entryPtr, nil
	}

	entry, err := l.storage.Entries(index, index+1)
	if err != nil {
		return nil, err
	}
	return &entry[0], nil
}

func (l *RaftLog) getEntryFromRaftLogIfExist(index uint64) *pb.Entry {
	if !l.isInRaftLogEntriesRange(index) {
		return nil
	}
	return &l.entries[index-l.entries[0].Index]
}

func (l *RaftLog) isInRaftLogEntriesRange(index uint64) bool {
	if len(l.entries) == 0 {
		return false
	}
	return l.entries[0].Index <= index && l.entries[len(l.entries)-1].Index >= index
}

// Entries from raftlog if low or hight not in range it will panic
// 将raftlog 视为数组
// 上层raft 算法模块查询Entries 一般是sendAppend时要获取数据
// 应当会有报错 表示Entries已经被gc了
func (l *RaftLog) Entries(low, high uint64) []pb.Entry {
	if low > high {
		panic(fmt.Errorf("Entries low>hight %v %v fail", low, high))
	}
	ents := []pb.Entry{}
	for i := low; i < high; i++ {
		ent, err := l.GetEntry(i)
		// TODO return error?
		if err != nil {
			panic(err)
		}
		ents = append(ents, *ent)
	}
	return ents
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

// log中是否为空 （初始化时）
func (l *RaftLog) IsEmpty() bool {
	return len(l.entries) == 0
}

func (l *RaftLog) commit(index uint64) {
	// log.Debugf("log commit 1 %v", l.String())
	l.committed = index
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) String() string {
	status := fmt.Sprintf(`Log { initIndex: %v,initTerm: %v,commit: %v, stable: %v,apply: %v, lastindex: %v,entry_len: %v,entries: %v}`,
		l.initIndex,
		l.initTerm,
		l.committed,
		l.stabled,
		l.applied,
		l.LastIndex(),
		len(l.entries),
		ShowEntries(l.entries),
	)
	return status
}

func (l *RaftLog) MetaString() string {
	status := fmt.Sprintf(`Log { initIndex: %v,initTerm: %v,commit: %v, stable: %v,apply: %v, lastindex: %v,entry_len: %v}`,
		l.initIndex,
		l.initTerm,
		l.committed,
		l.stabled,
		l.applied,
		l.LastIndex(),
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
	if l.applied > l.committed {
		panic("raftLog  unAppliedEntis apply > commit ??")
	}
	return l.Entries(l.applied+1, l.committed+1)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if l.isInRaftLogEntriesRange(i) {
		entry, err := l.GetEntry(i)
		if err != nil {
			return 0, err
		}
		return entry.Term, nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) clearEntries() {
	l.entries = []pb.Entry{}
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
// TODO check all uint64 is necessey
// return hasConflict,conflictOffset
//  当hasConflict为true 时  serverLog[conflictOffset]为两者发生冲突的entry
func findConflict(serverLog *[]pb.Entry, followerLog *[]pb.Entry) (bool, uint64) {
	if len(*serverLog) != len(*followerLog) {
		panic("fincConflict server entries len should eq follower entries len")
	}
	if len(*serverLog) == 0 && len(*followerLog) == 0 {
		return false, uint64(0)
	}
	for i := 0; i < len(*serverLog); i++ {
		if (*serverLog)[i].Index != (*followerLog)[i].Index || (*serverLog)[i].Term != (*followerLog)[i].Term {
			return true, uint64(i)
		}
	}
	return false, uint64(0)
}

// TestFollowerAppendEntries2AB
// TestHandleMessageType_MsgAppend2AB
func (l *RaftLog) tryAppendEntries(preLogIndex uint64, preLogTerm uint64, entries []*pb.Entry) (uint64, error) {
	// 当我们收到server发来的Entries时 有几种情况需要分类讨论
	// 首先保证 server的Entries是合法的

	if preLogIndex > l.LastIndex() || l.GetTerm(preLogIndex) != preLogTerm {
		return 0, fmt.Errorf("could not find this index and term")
	}

	// 如果实际上server并没有发送 entries过来 那么实际上server和leader已经match 的部分就是prelogIndex本身
	// 这可能是在确认真正要的发的entries的起始地址
	if len(entries) == 0 {
		return preLogIndex, nil
	}

	// 实际上只可能有4种情况
	// |--------|
	//   |---|

	// |--------|
	//      |---|

	//   |--------|
	//          |---|

	// |--------|
	//          |---|

	// TODO ??? we have to do this?
	serverEntries := []pb.Entry{}
	for _, e := range entries {
		serverEntries = append(serverEntries, *e)
	}

	// case 4. server发来的Entries正好接到follower Entries的后边
	if preLogIndex == l.LastIndex() {
		l.entries = append(l.entries, serverEntries...)
		return l.entries[len(l.entries)-1].Index, nil
	}

	// 还有种方法是 findConflict 直接传入 两个引用和一个长度
	// 取出两者重复的部分开始比较
	endIndex := min(preLogIndex+uint64(len(entries)), l.LastIndex())
	// 从prelog后面的一个entry 一直拿到endIndex
	followerDuplicateEntries := l.Entries(preLogIndex+1, endIndex+1)
	// 和followerDuplicateEntries 拿同样多即可
	serverDuplicateEntries := serverEntries[0 : 0+len(followerDuplicateEntries)]

	hasConflict, conflictOffset := findConflict(&followerDuplicateEntries, &serverDuplicateEntries)

	log.Debugf("hasConflict conflictOffset %v %v ", hasConflict, conflictOffset)
	if hasConflict {

		// 有冲突时 我们可能需要更改stable Index
		// serverEntries 与 server duplicate是相同起点的 所以conflictoffset指向的是同一值
		conflictIndex := serverEntries[conflictOffset].Index
		l.stabled = min(l.stabled, conflictIndex-1)

		// TODO  question   根据TestFollowerAppendEntries2AB的暗示 似乎不能将entries设成unstable的部分？
		// 冲突是在log.entries中的
		if len(l.entries) > 0 && conflictIndex >= l.entries[0].Index {
			l.entries = l.entries[0 : 0+conflictIndex-l.entries[0].Index]
			l.entries = append(l.entries, serverEntries[conflictOffset:]...)
		} else {
			// 冲突是在storage中的
			l.entries = serverEntries[conflictOffset:]
		}
		return l.entries[len(l.entries)-1].Index, nil

	} else {
		if l.LastIndex() < serverEntries[len(serverEntries)-1].Index {
			l.entries = append(l.entries, serverEntries[l.LastIndex()-serverEntries[0].Index+1:]...)
			return l.entries[len(l.entries)-1].Index, nil
		}
		//  follower的log完全包含了server的log 此时 两者确定的匹配的index是server发来的最后一个index
		return serverEntries[len(serverEntries)-1].Index, nil
	}
}
