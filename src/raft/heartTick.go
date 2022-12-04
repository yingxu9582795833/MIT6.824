package raft

import (
	"sync"
)

//心跳包

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	State State
	Term  int
	Index int
}

//AppendEntries 如果同时存在两个相同任期的leader则会出错，幸运的是，不会存在（因为raft总是奇数）
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		DPrintf(Func{fType: AppendEntries, op: Success}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		rf.toBeFollower(args.Term)
		reply.State = success
	} else {
		DPrintf(Func{fType: AppendEntries, op: Rejected}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		reply.State = rejected
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendAppendEntries(server int, isRejected *bool, cond *sync.Cond) bool {
	rf.mu.RLock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	rf.mu.RUnlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		reply.State = lose
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.State {
	case lose:
		DPrintf(Func{fType: heartTick, op: Lose}, rf.me, reply.Index, reply.Term, rf.currentTerm)
	case success:
		DPrintf(Func{fType: heartTick, op: Success}, rf.me, reply.Index, reply.Term, rf.currentTerm)
	case rejected:
		DPrintf(Func{fType: heartTick, op: Rejected}, rf.me, reply.Index, reply.Term, rf.currentTerm)
		rf.toBeFollower(reply.Term)
	}
	return ok
}

func (rf *Raft) startHeart() {
	rf.mu.RLock()
	//假设leader1，任期为2,失联，新leader2任期为3。leader1重联，被leader2更新为follower，此时heartTime定时器刚好触发，随后才被leader2重置heartTime定时器
	//于是leader1开始发送心跳
	//本质是因为heartTime定时器触发和leader3重置定时器是并行的
	//所以需要加这条判断语句防止这种情况
	if rf.status != Leader {
		rf.mu.RUnlock()
		return
	}
	isRejected := false
	rf.heartTimer.start()
	rf.mu.RUnlock()
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, &isRejected, cond)
	}
}
