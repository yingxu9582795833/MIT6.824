package raft

type toBeFollower struct {
	raft *Raft
}

func (tr *toBeFollower) transfer(arg ...interface{}) SMState {
	tr.raft.mu.Lock()
	defer tr.raft.mu.Unlock()
	rf := tr.raft
	if len(arg) > 1 {
		return ErrorState
	}
	rf.heartTimer.stop()
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
	context, ok := arg[0].(*higherTerm)
	//已被其他rf先一步更新，则不进行状态的改变
	if ok {
		switch context.action {
		case appendEntriesSucceed, appendEntriesRejected:
			rf.currentTerm = context.highTerm
			rf.votedFor = -1
			rf.voteNum = 0
		case electionRejected:
			rf.currentTerm = context.highTerm
			rf.votedFor = -1
			rf.voteNum = 0
		case electionSucceed:
			//已经被其他索票更新了
			if rf.currentTerm >= context.highTerm {
				return notTransfer
			}
			rf.currentTerm = context.highTerm
			rf.votedFor = context.highIndex
			rf.voteNum = 0
		}
	} else {
		return ErrorState
	}
	return Follower
}

type toBeLeader struct {
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
