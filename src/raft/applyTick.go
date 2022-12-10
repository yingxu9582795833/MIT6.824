package raft

import (
	"time"
)

const applyTime = 25

func (rf *Raft) applyTick() {
	ticker := time.NewTicker(applyTime * time.Millisecond)
	for rf.killed() == false {
		<-ticker.C
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0) //为了尽快解锁，先用一个切片将msg存起来，解锁之后再发送
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()
		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}
}
