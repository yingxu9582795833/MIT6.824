package raft

import (
	"sync"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CurrentTerm int //当前任期
	Ind         int //服务器索引
}

type RequestVoteReply struct {
	// Your data here (2A).
	State State
	Term  int
	Index int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//如果当前任期大于收到任期，则不投票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Index = rf.me
	if args.CurrentTerm < rf.currentTerm {
		reply.State = rejected
		reply.Term = rf.currentTerm
		DPrintf(Func{fType: RequestVote, op: Rejected}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		return
	} else if args.CurrentTerm > rf.currentTerm {
		DPrintf(Func{fType: RequestVote, op: Success}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		rf.voteToCandidate(args, reply)
	} else if rf.votedFor == -1 || rf.votedFor == args.Ind { //args.currentTerm == rf.currentTerm且没投过票
		DPrintf(Func{fType: RequestVote, op: Success}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		rf.voteToCandidate(args, reply)
	} else {
		reply.State = voted
	}
}

func (rf *Raft) sendRequestVote(server int, joinCount *int, elected *bool, cond *sync.Cond) bool {
	rf.mu.RLock()
	args := RequestVoteArgs{
		CurrentTerm: rf.currentTerm,
		Ind:         rf.me,
	}
	reply := RequestVoteReply{}
	rf.mu.RUnlock()
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		reply.State = lose
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.State {
	case lose:
		DPrintf(Func{fType: sendRequestVote, op: Lose}, rf.me, server)
	case voted:
		DPrintf(Func{fType: sendRequestVote, op: Voted}, rf.me, reply.Index, reply.Term, rf.currentTerm)
	case success: //[requestVote][requestVote]
		rf.voteNum++
		DPrintf(Func{fType: sendRequestVote, op: Success}, rf.me, reply.Index, reply.Term, rf.currentTerm, rf.voteNum)
		if rf.voteNum > len(rf.peers)/2 {
			cond.L.Lock()
			if !*elected {
				rf.toBeLeader()
				*elected = true
				cond.Broadcast()
			}
			cond.L.Unlock()
		}
	case rejected:
		DPrintf(Func{fType: sendRequestVote, op: Rejected}, rf.me, reply.Index, reply.Term, rf.currentTerm)
		rf.toBeFollower(reply.Term)
	}
	cond.L.Lock()
	*joinCount++
	cond.L.Unlock()
	return ok
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	//存在的情况和startHeart类似
	if rf.status == Leader {
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
