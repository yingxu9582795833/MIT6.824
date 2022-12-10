package raft

func (rf *Raft) toBeFollower(newTerm int) {
	rf.heartTimer.stop()
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
	}
	rf.role = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
}

func (rf *Raft) toBeCandidate() {
	rf.voteNum = 0
	rf.role = Candidate
	rf.currentTerm++
	rf.voteNum++
	rf.votedFor = rf.me
	rf.persist()
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
}

func (rf *Raft) toBeLeader() {
	rf.role = Leader
	rf.electionTimer.stop()
	rf.heartTimer.start()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) voteToCandidate(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.State = voteSuccess
	reply.Term = rf.currentTerm
	rf.electionTimer.setWaitTime(RandElection())
	rf.electionTimer.start()
	rf.currentTerm = args.Term
	rf.voteNum = 0
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.role = Follower
}
