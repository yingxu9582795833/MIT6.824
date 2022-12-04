package raft

import "sync"

type heartTicker struct {
	raft *Raft
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartTimer.stop()
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
	if rf.currentTerm < args.Term {
		DPrintf(Func{fType: AppendEntries, op: Success}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		rf.eventCh <- &higherTerm{Index: higherTermIndex, highTerm: args.Term, action: appendEntriesSucceed}
		reply.State = success
	} else if rf.currentTerm > args.Term {
		DPrintf(Func{fType: AppendEntries, op: Rejected}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		reply.State = rejected
	} else {
		DPrintf(Func{fType: AppendEntries, op: Success}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		rf.eventCh <- &higherTerm{Index: higherTermIndex, highTerm: args.Term, action: appendEntriesSucceed}
		reply.State = success
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendAppendEntries(server int, isRejected *bool, cond *sync.Cond) {
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
	switch reply.State {
	case lose:
	case success:
	case rejected:
		cond.L.Lock()
		if !*isRejected {
			rf.eventCh <- &higherTerm{
				Index:    higherTermIndex,
				highTerm: reply.Term,
				action:   appendEntriesRejected,
			}
			*isRejected = true
		}
		cond.L.Unlock()
	}
}
func (rf *Raft) startHeartTick() {
	isRejected := false
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, &isRejected, cond)
	}
}
func (tr *heartTicker) transfer(args ...interface{}) SMState {
	//每个人发心跳包
	tr.raft.mu.Lock()
	defer tr.raft.mu.Unlock()
	rf := tr.raft
	rf.electionTimer.stop()
	rf.heartTimer.start()
	go rf.startHeartTick()
	return Leader
}
