package raft

import (
	"sync"
	"time"
)

type startElection struct {
	StateMachine
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
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//如果当前任期大于收到任期，则不投票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CurrentTerm < rf.currentTerm {
		reply.State = rejected
		DPrintf(Func{fType: RequestVote, op: Rejected}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		return
	} else if rf.votedFor != -1 && rf.currentTerm == args.CurrentTerm { //否则投票
		reply.State = used
	} else {
		//重置定时器
		rf.electionTimer.Reset(RandElection())
		DPrintf(Func{fType: RequestVote, op: Success}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		rf.currentTerm = args.CurrentTerm
		rf.votedFor = args.Ind
		rf.status = Follower
		reply.State = voted
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, joinCount *int, cond sync.Cond) bool {
	//反复寻求投票
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		tim := time.NewTimer(time.Duration(rf.loseTime) * time.Millisecond)
		for !ok {
			select {
			case <-tim.C:
				reply.State = timeout
				break
			default:
				ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
			}
		}
	}
	cond.L.Lock()
	*joinCount++
	cond.L.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.State {
	case timeout:
	case success:
		rf.voteNum++
		if rf.voteNum > len(rf.peers)+1:

	case rejected:
	}
	return ok
}
func (rf *Raft) startElect() {
	joinCount := 0
	rf.mu.RLock()
	args := RequestVoteArgs{
		CurrentTerm: rf.currentTerm,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{}
	rf.mu.RUnlock()
	cond := sync.NewCond(&sync.Mutex{}) //条件变量，选举出领导或者给所有peer索票后退出
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go sendRequestVote(rf.me,&args, &reply,&joinConut,cond)
	}
	cond.L.Lock()
	if joinCount < len(rf.peers)+1{
		cond.Wait()
	}
	cond.L.Unlock()
}
func (tr *startElection) transfer() SMState {
	//
	tr.raft.mu.Lock()
	rf := tr.raft
	rf.currentTerm++
	rf.voteNum++
	rf.votedFor = rf.me
	tr.raft.mu.Unlock()
	go rf.startElect()
	return Candidate
}
