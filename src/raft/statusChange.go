package raft

type toBeFollower struct {
	StateMachine
	raft *Raft
}

func (tr *toBeFollower) transfer(arg ...interface{}) SMState {
	tr.raft.mu.Lock()
	defer tr.raft.mu.Unlock()
	rf := tr.raft
	if len(arg) > 1 {
		return ErrorState
	}
	context, ok := arg[0].(higherTerm)
	if ok {
		switch context.action {
		case appendEntriesRejected:
			rf.currentTerm = context.highTerm
			rf.votedFor = -1
			rf.voteNum = 0
			//重置定时器
			rf.electionTimer.setWaitTime(RandElection())
			rf.electionTimer.start()
		case electionRejected:
			rf.currentTerm = context.highTerm
			rf.votedFor = context.highIndex
			rf.voteNum = 0
			rf.electionTimer.setWaitTime(RandElection())
			rf.electionTimer.start()
		}
	} else {
		return ErrorState
	}
	return Follower
}

type toBeLeader struct {
	StateMachine
	raft *Raft
}

func (tr *toBeLeader) transfer(arg ...interface{}) SMState {
	tr.raft.mu.Lock()
	defer tr.raft.mu.Unlock()
	rf := tr.raft
	//停止选举定时器
	rf.electionTimer.stop()
	//开始心跳定时器
	rf.heartTimer.start()
	return Leader
}
