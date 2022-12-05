package raft

func (rf *Raft) toBeFollower(newTerm int) {
	rf.heartTimer.stop()
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
	}
	rf.status = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
}

func (rf *Raft) toBeCandidate() {
	rf.voteNum = 0
	rf.currentTerm++
	rf.voteNum++
	rf.votedFor = rf.me
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
}

func (rf *Raft) toBeLeader() {
	rf.status = Leader
	rf.electionTimer.stop()
	rf.heartTimer.start()
}

func (rf *Raft) voteToCandidate(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.State = success
	reply.Term = rf.currentTerm
	reply.Index = rf.me
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.votedFor = args.CandidateId
	rf.status = Follower
}
