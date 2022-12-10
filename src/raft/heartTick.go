package raft

import (
	"sync"
)

//心跳包

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int        //新日志前一条日志的索引
	PreLogTerm   int        //新日志前一条日志的任期
	Entries      []LogEntry //需要被保存的日志
	LeaderCommit int        //Leader已知已提交的日志最高索引
}

type AppendEntriesReply struct {
	State        AppendEntryState
	Term         int
	NewNextIndex int
}

func (rf *Raft) getPreLogIndex(server int) int {
	return rf.nextIndex[server] - 1
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
		DPrintf(Func{fType: AppendEntries, op: Rejected}, args.LeaderId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
			args.PreLogIndex, args.PreLogTerm, args.Entries)
		reply.State = appendEntryOutOfDate
		return
	}
	rf.toBeFollower(args.Term) //该节点回归follower的状态
	//在Leader存在日志的情况下，有两种需要nextIndex回退的可能
	//1.caller.PreLogIndex > rf.getLastIndex() 意味着follower节点存在日志缺失的情况，需要PreLogIndex回退
	//2.caller.PreLogIndex在follower节点中所对应的任期和Leader不一致，也需要PreLogIndex回退
	//上诉两种情况对应论文中的一致性检查
	if args.PreLogTerm >= 1 && (args.PreLogIndex > rf.getLastIndex() || rf.logs[args.PreLogIndex].Term != args.PreLogTerm) {
		DPrintf(Func{fType: AppendEntries, op: Conflict}, args.LeaderId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
			args.PreLogIndex, args.PreLogTerm, args.Entries)
		reply.State = appendEntryConflict
		reply.NewNextIndex = rf.commitIndex + 1
		return
	}
	//leader1 append Slave1成功 但是因为网络原因rpc返回失败了，leader1误以为slave1并没有接收到心跳，leader1对slave1维护的nextIndex并没有更新，导致之后再append slave1时
	//会出现args.PreLogIndex < rf.commitIndex的情况
	if args.PreLogTerm != 0 && (args.PreLogIndex < rf.commitIndex) {
		DPrintf(Func{fType: AppendEntries, op: Exceed}, args.LeaderId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
			args.PreLogIndex, args.PreLogTerm, args.Entries)
		reply.State = appendEntryExceed
		reply.NewNextIndex = rf.commitIndex + 1
		return
	}
	reply.State = appendEntrySuccess
	rf.logs = rf.logs[0 : args.PreLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	rf.persist()
	reply.NewNextIndex = len(rf.logs)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	DPrintf(Func{fType: AppendEntries, op: Success}, args.LeaderId, rf.me, rf.currentTerm, args.Term, rf.getLastIndex(), rf.getLastTerm(),
		args.PreLogIndex, args.PreLogTerm, args.Entries, rf.logs, rf.commitIndex)
}
func (rf *Raft) sendAppendEntries(server int, copyNum *int, tempMutex *sync.Mutex) bool {
	rf.mu.RLock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  rf.getPreLogIndex(server),
		PreLogTerm:   rf.getPreLogTerm(server),
		LeaderCommit: rf.commitIndex,
	}
	args.Entries = make([]LogEntry, len(rf.logs[args.PreLogIndex+1:]))
	//深拷贝，防止数据冲突
	copy(args.Entries, rf.logs[args.PreLogIndex+1:])
	//args.Entries = rf.logs[args.PreLogIndex+1:]
	reply := AppendEntriesReply{}
	rf.mu.RUnlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		reply.State = appendEntryLose
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return ok
	}
	switch reply.State {
	case appendEntryLose:
		DPrintf(Func{fType: sendAppendEntries, op: Lose}, rf.me, server)
		rf.nextIndex[server] = len(rf.logs)
	case appendEntrySuccess:
		rf.nextIndex[server] = reply.NewNextIndex
		rf.matchIndex[server] = reply.NewNextIndex - 1
		for index := rf.getLastIndex(); index >= rf.commitIndex+1; index-- {
			sum := 0
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					sum++
				} else {
					//这个地方是为了符合论文图8
					if rf.matchIndex[i] >= index && rf.logs[index].Term == rf.currentTerm {
						sum++
					}
				}
				if sum > len(rf.peers)/2 {
					rf.commitIndex = index
					return ok
				}
			}
		}
		DPrintf(Func{fType: sendAppendEntries, op: Success}, rf.me, server, reply.Term, rf.currentTerm,
			rf.logs, rf.commitIndex, rf.nextIndex[server], rf.matchIndex[server])
	case appendEntryOutOfDate:
		DPrintf(Func{fType: sendAppendEntries, op: Rejected}, rf.me, server, reply.Term, rf.currentTerm)
		rf.toBeFollower(reply.Term)
	case appendEntryConflict:
		rf.nextIndex[server] = reply.NewNextIndex
		DPrintf(Func{fType: sendAppendEntries, op: Conflict}, rf.me, server, reply.Term, rf.currentTerm,
			rf.logs, rf.commitIndex, rf.nextIndex[server])
	case appendEntryExceed:
		rf.nextIndex[server] = reply.NewNextIndex
		DPrintf(Func{fType: sendAppendEntries, op: Exceed}, rf.me, server, reply.Term, rf.currentTerm,
			rf.logs, rf.commitIndex, rf.nextIndex[server])
	}
	return ok
}

func (rf *Raft) startHeart() {
	rf.mu.RLock()
	//假设leader1，任期为2,失联，新leader2任期为3。leader1重联，被leader2更新为follower，此时heartTime定时器刚好触发，随后才被leader2重置heartTime定时器
	//于是leader1开始发送心跳
	//本质是因为heartTime定时器触发和leader3重置定时器是并行的
	//所以需要加这条判断语句防止这种情况
	if rf.role != Leader {
		rf.mu.RUnlock()
		return
	}
	copyNum := 0
	rf.heartTimer.start()
	rf.mu.RUnlock()
	tempMutex := sync.Mutex{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.sendAppendEntries(i, &copyNum, &tempMutex)
	}
}
