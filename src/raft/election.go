package raft

import (
	"sync"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //当前任期
	CandidateId  int //服务器索引
	LastLogIndex int //候选人最后一日志的索引
	LastLogTerm  int //候选人最后一条日志的任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	State VoteState
	Term  int
}

func (rf *Raft) lessNewThan(Term int, Index int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return Term > lastTerm || (Term == lastTerm && Index >= lastIndex)
}
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}
func (rf *Raft) getLastTerm() int {
	return rf.logs[rf.getLastIndex()].Term
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//如果当前任期大于收到任期，则不投票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if args.Term < rf.currentTerm {
	//	reply.State = voteRejected
	//	reply.Term = rf.currentTerm
	//	DPrintf(Func{fType: RequestVote, op: Rejected}, args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
	//		args.LastLogIndex, args.LastLogTerm)
	//	return
	//}
	//if args.Term > rf.currentTerm {
	//	rf.toBeFollower(args.Term)
	//}
	//if !rf.lessNewThan(args.LastLogIndex, args.LastLogTerm) || // 判断日志是否conflict
	//	rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { // paper中的第二个条件votedFor is null
	//	reply.State = voteRejected
	//	reply.Term = rf.currentTerm
	//	DPrintf(Func{fType: RequestVote, op: Rejected}, args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
	//		args.LastLogIndex, args.LastLogTerm)
	//	return
	//} else {
	//	DPrintf(Func{fType: RequestVote, op: Success}, args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
	//		args.LastLogIndex, args.LastLogTerm)
	//	rf.voteToCandidate(args, reply)
	//	return
	//}
	//必要条件，否则可能导致某slave节点任期永远不会更新
	if args.Term > rf.currentTerm {
		rf.toBeFollower(args.Term)
	}
	if args.Term > rf.currentTerm && rf.lessNewThan(args.LastLogTerm, args.LastLogIndex) {
		DPrintf(Func{fType: RequestVote, op: Success}, args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
			args.LastLogIndex, args.LastLogTerm)
		rf.voteToCandidate(args, reply)
	} else if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.lessNewThan(args.LastLogTerm, args.LastLogIndex) {
		DPrintf(Func{fType: RequestVote, op: Success}, args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
			args.LastLogIndex, args.LastLogTerm)
		rf.voteToCandidate(args, reply)
	} else { // args.Term < rf.currentTerm || other situation
		reply.State = voteRejected
		reply.Term = rf.currentTerm
		DPrintf(Func{fType: RequestVote, op: Rejected}, args.CandidateId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
			args.LastLogIndex, args.LastLogTerm)
	}
}

func (rf *Raft) sendRequestVote(server int, joinCount *int, elected *bool, cond *sync.Cond) bool {
	rf.mu.RLock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	reply := RequestVoteReply{}
	rf.mu.RUnlock()
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		reply.State = voteLose
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Follower {
		return ok
	}
	switch reply.State {
	case voteLose:
		DPrintf(Func{fType: sendRequestVote, op: Lose}, rf.me, server)
	case voteSuccess: //[requestVote][requestVote]
		rf.voteNum++
		DPrintf(Func{fType: sendRequestVote, op: Success}, rf.me, server, reply.Term, rf.currentTerm, rf.voteNum)
		if rf.voteNum > len(rf.peers)/2 {
			cond.L.Lock()
			if !*elected {
				rf.toBeLeader()
				*elected = true
				cond.Broadcast()
			}
			cond.L.Unlock()
		}
	case voteRejected:
		DPrintf(Func{fType: sendRequestVote, op: Rejected}, rf.me, server, reply.Term, rf.currentTerm)
		rf.toBeFollower(reply.Term)
	default: //意料之外，则输出该条目，方便debug
		DPrintf(Func{fType: sendRequestVote, op: Unexpected}, rf.me, server)
	}
	cond.L.Lock()
	*joinCount++
	cond.L.Unlock()
	return ok
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//存在的情况和startHeart类似
	if rf.role == Leader {
		rf.mu.Unlock()
		return
	}
	rf.toBeCandidate()
	rf.mu.Unlock()
	joinCount := 0
	elected := false
	cond := sync.NewCond(&sync.Mutex{}) //条件变量，选举出领导或者给所有peer索票后退出
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &joinCount, &elected, cond)
	}
	cond.L.Lock()
	if joinCount+1 < len(rf.peers) {
		cond.Wait()
	}
	cond.L.Unlock()
}
