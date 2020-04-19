package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

type toERaftPb interface {
	ToPb() pb.Message
	FromPb(pb.Message)
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     uint64
	To           uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*pb.Entry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	From              uint64
	To                uint64
	Term              uint64
	FollowerLastIndex uint64
	Success           bool
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	From        uint64
	To          uint64
	Term        uint64
	VoteGranted bool
}

func (m *AppendEntriesRequest) ToPb() pb.Message {
	msg := pb.Message{}
	msg.Term = m.Term
	msg.From = m.LeaderID
	msg.To = m.To
	msg.Index = m.PrevLogIndex
	msg.LogTerm = m.PrevLogTerm
	msg.Entries = m.Entries
	msg.Commit = m.LeaderCommit
	msg.MsgType = pb.MessageType_MsgAppend
	return msg
}

func (m *AppendEntriesRequest) FromPb(msg pb.Message) {
	m.Term = msg.Term
	m.LeaderID = msg.From
	m.To = msg.To
	m.PrevLogIndex = msg.Index
	m.PrevLogTerm = msg.LogTerm
	m.Entries = msg.Entries
	m.LeaderCommit = msg.Commit
}

func (m *AppendEntriesResponse) ToPb() pb.Message {
	msg := pb.Message{}
	msg.Term = m.Term
	msg.From = m.From
	msg.To = m.To
	msg.Reject = !m.Success
	msg.MsgType = pb.MessageType_MsgAppendResponse
	return msg
}

func (m *AppendEntriesResponse) FromPb(msg pb.Message) {
	m.Term = msg.Term
	m.From = msg.From
	m.To = msg.To
	m.Success = !msg.Reject
}

func (m *RequestVoteRequest) ToPb() pb.Message {
	msg := pb.Message{}
	msg.Term = m.Term
	msg.From = m.CandidateId
	msg.Index = m.LastLogIndex
	msg.LogTerm = m.LastLogTerm
	msg.MsgType = pb.MessageType_MsgRequestVote
	return msg
}

func (m *RequestVoteRequest) FromPb(msg pb.Message) {
	m.Term = msg.Term
	m.CandidateId = msg.From
	m.LastLogIndex = msg.Index
	m.LastLogTerm = msg.LogTerm
}

func (m *RequestVoteResponse) ToPb() pb.Message {
	msg := pb.Message{}
	msg.Term = m.Term
	msg.From = m.From
	msg.To = m.To
	msg.Reject = !m.VoteGranted
	msg.MsgType = pb.MessageType_MsgRequestVoteResponse
	return msg
}

func (m *RequestVoteResponse) FromPb(msg pb.Message) {
	m.Term = msg.Term
	m.From = msg.From
	m.To = msg.To
	m.VoteGranted = !msg.Reject
}
