package raft

import "sync"

type heartTicker struct {
	StateMachine
	raft *Raft
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		DPrintf(Func{fType: AppendEntries, op: Success}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteNum = 0
		rf.heartTimer.stop()
		rf.electionTimer.setWaitTime(RandElection())
		rf.electionTimer.start()
		reply.State = success
	} else {
		DPrintf(Func{fType: AppendEntries, op: Rejected}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		reply.State = rejected
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendAppendEntries(server int, heartCount *int, cond *sync.Cond) {
	//fmt.Printf("%v sendAppendEntires to  start %v\n", rf.me, server)
	rf.mu.RLock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	rf.mu.RUnlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("%v sendAppendEntires to %v end \n", rf.me, server)
	if !ok {
		reply.State = lose
	}
	switch reply.State {
	case lose:
	case success:
	case rejected:
		rf.eventCh <- &higherTerm{
			Index:    higherTermIndex,
			highTerm: reply.Term,
			action:   appendEntriesRejected,
		}
	}
}
func (rf *Raft) startHeartTick() {
	heartCount := 0
	cond := sync.NewCond(&sync.Mutex{})
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(rf.me, &heartCount, cond)
	}
	cond.L.Lock()
	if heartCount < len(rf.peers) {
		cond.Wait()
	}
	cond.L.Unlock()
}
func (tr *heartTicker) transfer(args interface{}) SMState {
	//每个人发心跳包
	tr.raft.mu.Lock()
	defer tr.raft.mu.Unlock()
	rf := tr.raft
	rf.heartTimer.start()
	go rf.startHeartTick()
	return Leader
}
