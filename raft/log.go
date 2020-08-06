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
	// entries[i].index = entries[0].index + i
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	initIndex uint64
	initTerm  uint64
	tag       string
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	l := new(RaftLog)
	l.storage = storage
	l.entries = []pb.Entry{}

	firstIndex, error := storage.FirstIndex()
	if error != nil {
		panic(error)
	}

	lastIndex, error := storage.LastIndex()

	if error != nil {
		panic(error)
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

	// TODO 我们必须将所有的storage中的 entries全部放在 entries中 否则TestFollowerAppendEntries2AB 会出问题
	if lastIndex >= firstIndex {
		// log.Infof("tag: snap,log: load entries %v %v\n", firstIndex, lastIndex+1)
		entries, error := storage.Entries(firstIndex, lastIndex+1)
		if error != nil {
			log.Errorf("load entries fail %v %v %v %v\n", firstIndex, lastIndex, l.StableRangeString(), error)
			panic(error)
		}
		l.entries = append(l.entries, entries...)
	}
	return l
}

// rawnode发现有pendingSnapshot会Ready 最终在peer_msg_handle中将其Apply
func (l *RaftLog) savePendingSnapshot(snapshot *pb.Snapshot) {
	l.pendingSnapshot = snapshot
}

func (l *RaftLog) SetTag(tag string) {
	l.tag = tag
}

// LastIndex return the last index of the log entries
// 如果有snapshot的话 lastindex是snapshot的index
// Q: 为什么
// A:
// 1. snapshot的index一定比当前的lastIndex大
// 2. 假设leader在发送snapshot后又发送了snapshot之后的entries 两者在follower中正好一同处理(rawnode没有及时Ready) 如果lastIndex不是snapshot的index 那么就后面的entries的preLog和preTerm就无法匹配
// 后面的Term也是相同的原因
// TestRestoreSnapshot2C
func (l *RaftLog) LastIndex() uint64 {
	log.Infof("raft_id: %v,tag: ,log: %v %v %v \n", l.tag, l.UnstableRangeString(), l.StableRangeString(), l.PendingSnapshotStatus())
	// TODO 3个max?
	if l.pendingSnapshot != nil {
		if len(l.entries) != 0 && l.entries[len(l.entries)-1].Index > l.pendingSnapshot.Metadata.Index {
			return l.entries[len(l.entries)-1].Index
		}
		return l.pendingSnapshot.Metadata.Index
	}

	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}
	storageLastIndex, err := l.storage.LastIndex()
	if err != nil {
		log.Infof("raft_id: %v,log: snapshot and unstable entries not exits, get storage lastindex fail \n", err)
		panic(err)
	}
	return storageLastIndex
}

func (l *RaftLog) isValidIndex(index uint64) error {
	lastIndex := l.LastIndex()
	if index > lastIndex {
		return fmt.Errorf("invalid index index %v lastIndex %v", index, lastIndex)
	}
	if len(l.entries) != 0 && index >= l.entries[0].Index {
		return nil
	}
	storageFirstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return err
	}
	if index < storageFirstIndex {
		log.Warnf("raft_id: %v, log:invalid index index %v lastIndex  %v storageFirstIndex %v", l.tag, index, lastIndex, storageFirstIndex)
		return ErrCompacted
	}
	return nil
}

func (l *RaftLog) GetEntry(index uint64) (*pb.Entry, error) {
	err := l.isValidIndex(index)
	if err != nil {
		log.Infof("not valid\n")
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
func (l *RaftLog) Entries(low, high uint64) ([]pb.Entry, error) {
	log.Infof("raft_id: %v, log: entries %v %v %v %v %v\n", l.tag, low, high, l.UnstableRangeString(), l.StableRangeString(), l.PendingSnapshotStatus())
	if low > high {
		return nil, fmt.Errorf("Entries low>hight %v %v fail", low, high)
	}
	ents := []pb.Entry{}
	for i := low; i < high; i++ {
		ent, err := l.GetEntry(i)
		// TODO return error?
		if err != nil {
			return nil, err
		}
		ents = append(ents, *ent)
	}
	return ents, nil
}

// PtrEntries same as Entries but return []*pb.Entry instead of []pb.Entry
func (l *RaftLog) PtrEntries(low, hight uint64) ([]*pb.Entry, error) {
	entries, err := l.Entries(low, hight)
	if err != nil {
		return nil, err
	}

	ptrEntries := []*pb.Entry{}
	for i, _ := range entries {
		ptrEntries = append(ptrEntries, &entries[i])
	}
	return ptrEntries, nil
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
	// TODO
}

func (l *RaftLog) String() string {
	status := fmt.Sprintf(`Log { tag: %v, initIndex: %v,initTerm: %v,commit: %v, stable: %v,apply: %v, lastindex: %v ,%v, %v, %v}`,
		l.tag,
		l.initIndex,
		l.initTerm,
		l.committed,
		l.stabled,
		l.applied,
		l.LastIndex(),
		l.UnstableRangeString(),
		l.StableRangeString(),
		l.PendingSnapshotStatus(),
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

func (l *RaftLog) UnstableRangeString() string {
	if len(l.entries) == 0 {
		return fmt.Sprintf(`Unstable { len: %v,first: %v, last: %v }`, len(l.entries), "-", "-")
	}
	return fmt.Sprintf(`Unstable { len: %v,first: %v, last: %v }`, len(l.entries), l.entries[0].Index, l.entries[len(l.entries)-1].Index)
}

func (l *RaftLog) StableRangeString() string {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return fmt.Sprintf("Error:  StableRangeString could not get FirstIndex, err is %v", err)
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		return fmt.Sprintf("Error:  StableRangeString could not get lastIndex, err is %v", err)
	}
	return fmt.Sprintf(`Stable { len: %v,first: %v, last: %v }`, lastIndex-firstIndex, firstIndex, lastIndex)
}

func (l *RaftLog) PendingSnapshotStatus() string {
	if l.pendingSnapshot == nil {
		return fmt.Sprintf("PendingSnapshot not exist")
	}
	return fmt.Sprintf("PendingSnapshot {lastIndex: %v,lastTerm: %v}", l.pendingSnapshot.Metadata.Index, l.pendingSnapshot.Metadata.Term)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// unstable entries 只可能是 l.entries中的
	// 每次stabled总是在rawnode advance时被清空了
	// 当appendEntries发现conflict时 可能对通过 更新uentries 和调整stabled来调整unstableEntries
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	log.Infof("log: %v %v %v\n", l.stabled, l.UnstableRangeString(), l.StableRangeString())
	if l.stabled != 0 {
		return l.entries[l.stabled+1-l.entries[0].Index:]
	}
	return l.entries
}

func (l *RaftLog) Snapshot() (pb.Snapshot, error) {
	return l.storage.Snapshot()
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	return l.unAppliedEntis()
}

func (l *RaftLog) unAppliedEntis() (ents []pb.Entry) {
	// all of commited entry include the last commit entry
	if l.applied > l.committed {
		panic(fmt.Sprintf("raftLog  unAppliedEntis apply > commit %v %v ??", l.applied, l.committed))
	}
	ents, err := l.Entries(l.applied+1, l.committed+1)
	if err != nil {
		panic(err)
	}
	return ents
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	if len(l.entries) != 0 {
		log.Infof("term range %v %v\n", l.entries[0].Index, l.entries[len(l.entries)-1].Index)
	}

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
	// TODO why this??? 有些情况下 preLogIndex 被gc掉了????
	term, err := l.Term(preLogIndex)
	if err != nil {
		return 0, err
	}
	if preLogIndex > l.LastIndex() || term != preLogTerm {
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

	followerDuplicateEntries, err := l.Entries(preLogIndex+1, endIndex+1)
	if err != nil {
		panic(err)
	}
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
