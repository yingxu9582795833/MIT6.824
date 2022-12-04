package raft

import (
	"sync"
	"time"
)

type startElection struct {
	raft *Raft
}
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CurrentTerm int //当前任期
	CandidateId int //候选人id
}

type RequestVoteReply struct {
	State State
	Term  int
	Index int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//如果当前任期大于收到任期，则不投票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Index = rf.me
	if args.CurrentTerm > rf.currentTerm {
		DPrintf(Func{fType: RequestVote, op: Success}, args.CandidateId, rf.me, rf.currentTerm, args.CurrentTerm)
		reply.State = success
		reply.Term = rf.currentTerm
		reply.Index = rf.me
		rf.eventCh <- &higherTerm{Index: higherTermIndex, highTerm: args.CurrentTerm, highIndex: rf.me, action: electionSucceed}
	} else if args.CurrentTerm < rf.currentTerm { //投票
		reply.State = rejected
		DPrintf(Func{fType: RequestVote, op: Rejected}, args.CandidateId, rf.me, rf.currentTerm, args.CurrentTerm)
		reply.Term = rf.currentTerm
		reply.Index = rf.me
		return
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		DPrintf(Func{fType: RequestVote, op: Success}, args.CandidateId, rf.me, rf.currentTerm, args.CurrentTerm)
		reply.State = success
		reply.Term = rf.currentTerm
		reply.Index = rf.me
		rf.eventCh <- &higherTerm{Index: higherTermIndex, highTerm: args.CurrentTerm, highIndex: rf.me, action: electionSucceed}
	} else {
		reply.State = used
	}
}

//[caller(1)requestVote(3)] [caller(2)requestVote(3)] [caller(1) state change] [caller(2) state change]

func (rf *Raft) sendRequestVote(server int, joinCount *int, elected *bool, isRejected *bool, cond *sync.Cond) bool {
	//反复寻求投票
	rf.mu.RLock()
	args := RequestVoteArgs{
		CurrentTerm: rf.currentTerm,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{}
	rf.mu.RUnlock()
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		tim := time.NewTimer(time.Duration(rf.loseTime) * time.Millisecond)
		for !ok {
			select {
			case <-tim.C:
				reply.State = timeout
				break
			default:
				ok = rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			}
		}
	}
	cond.L.Lock()
	*joinCount++
	cond.L.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if *isRejected { //被拒绝了，则返回，不计算投票结果
		return ok
	}
	switch reply.State {
	case timeout:
	case used:
	case success:
		rf.voteNum++
		DPrintf(Func{fType: sendRequestVote, op: Success}, rf.me, reply.Index, reply.Term, rf.currentTerm, rf.voteNum)
		if rf.voteNum > len(rf.peers)/2 {
			cond.L.Lock()
			if !*elected {
				rf.eventCh <- &receiveMajority{Index: receiveMajorityIndex}
				*elected = true
				cond.Broadcast()
			}
			cond.L.Unlock()
		}
	case rejected:
		DPrintf(Func{fType: sendRequestVote, op: Rejected}, rf.me, reply.Index, reply.Term, rf.currentTerm)
		cond.L.Lock()
		if !*isRejected {
			rf.eventCh <- &higherTerm{Index: higherTermIndex, highTerm: reply.Term, highIndex: reply.Index, action: electionRejected}
			*isRejected = true
		}
		cond.L.Unlock()
	}
	return ok
}
func (rf *Raft) startElect() {
	joinCount := 0
	elected := false
	isRejected := false
	cond := sync.NewCond(&sync.Mutex{}) //条件变量，选举出领导或者给所有peer索票后退出
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &joinCount, &elected, &isRejected, cond)
	}
	cond.L.Lock()
	if joinCount < len(rf.peers)+1 {
		cond.Wait()
	}
	cond.L.Unlock()
}

//transfer bug highterm重置定时器
func (tr *startElection) transfer(args ...interface{}) SMState {
	tr.raft.mu.Lock()
	rf := tr.raft
	rf.voteNum = 0
	rf.currentTerm++
	rf.voteNum++
	rf.votedFor = rf.me
	rf.electionTimer.start()
	tr.raft.mu.Unlock()
	go rf.startElect()
	return Candidate
}
