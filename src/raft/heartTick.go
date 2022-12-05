package raft

import (
	"sync"
)

//心跳包

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int      //新日志前一条日志的索引
	PreLogTerm   int      //新日志前一条日志的任期
	Entries      []string //需要被保存的日志
	LeaderCommit int      //Leader已知已提交的日志最高索引
}

type AppendEntriesReply struct {
	State        State
	Term         int
	Index        int
	newNextIndex int
}

func (rf *Raft) getPreLogIndex(server int) int {
	return rf.nextIndex[server]
}
func (rf *Raft) getPreLogTerm(server int) int {
	return rf.logs[rf.getPreLogIndex(server)].Term
}

//AppendEntries 如果同时存在两个相同任期的leader则会出错，幸运的是，不会存在（因为raft总是奇数）
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term { //直接拒绝
		DPrintf(Func{fType: AppendEntries, op: Rejected}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		reply.State = rejected
		return
	}
	if args.PreLogIndex > rf.getLastIndex() || rf.logs[args.PreLogIndex].Term != args.PreLogIndex {
		reply.State = appendConfilict
		reply.newNextIndex = rf.lastApplied + 1
		return
	}
	rf.toBeFollower(args.Term)
}
func (rf *Raft) sendAppendEntries(server int, isRejected *bool, cond *sync.Cond) bool {
	rf.mu.RLock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  rf.getPreLogIndex(server),
		PreLogTerm:   rf.getPreLogTerm(server),
		LeaderCommit: rf.leaderCommit,
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
		DPrintf(Func{fType: sendAppendEntries, op: Lose}, rf.me, reply.Index)
	case success:
		DPrintf(Func{fType: sendAppendEntries, op: Success}, rf.me, reply.Index, reply.Term, rf.currentTerm)
	case rejected:
		DPrintf(Func{fType: sendAppendEntries, op: Rejected}, rf.me, reply.Index, reply.Term, rf.currentTerm)
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
