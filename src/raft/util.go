package raft

import (
	"fmt"
	"math/rand"
	"time"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()
}

// Debugging
const Debug = true

type FType int

const (
	RequestVote FType = iota
	sendRequestVote
	AppendEntries
	sendAppendEntries
	Test
	Start
)

type Op int

const (
	Rejected Op = iota
	Success
	Lose
	Voted
	Conflict
	Exceed
	Unexpected
	sendCommand
	disconnect
	connect
)

type Func struct {
	fType FType
	op    Op
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func DPrintf(f Func, a ...interface{}) {
	base := "[%-8v  %-45s]: "
	time := time.Since(debugStart).Microseconds()
	time /= 100
	if Debug {
		switch f.fType {
		case RequestVote:
			tBase := fmt.Sprintf("caller(%v)-RequestVote-rf(%v)", a[0], a[1])
			switch f.op {
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, rf.lastIndex : %v , rf.lastTerm : %v, "+
					"caller.preIndex : %v, caller.preTerm : %v\n", time, tBase+"-Success", a[2], a[3], a[4], a[5], a[6], a[7])
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, rf.lastIndex : %v , rf.lastTerm : %v, "+
					"caller.preIndex : %v, caller.preTerm : %v\n", time, tBase+"-Reject", a[2], a[3], a[4], a[5], a[6], a[7])
			case Unexpected:
				fmt.Printf(base+"\n", time, tBase+"-Unexpected")
			}
		case sendRequestVote:
			tBase := fmt.Sprintf("caller(%v)-sendRequestVote-rf(%v)", a[0], a[1])
			switch f.op {
			case Lose:
				fmt.Printf(base+"\n", time, tBase+"-Lose")
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Reject", a[2], a[3])
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, caller.voteNum: %v\n", time, tBase+"-Success", a[2], a[3], a[4])
			case Voted:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Voted", a[2], a[3])
			case Unexpected:
				fmt.Printf(base+"\n", time, tBase+"-Unexpected")
			}
		case Start:
			tBase := fmt.Sprintf("caller(%v)-Start", a[0])
			switch f.op {
			case sendCommand:
				fmt.Printf(base+"index: %v, term: %v, isLeader: %v\n", time, tBase+"-sendCommand", a[1], a[2], a[3])
			}
		case AppendEntries:
			tBase := fmt.Sprintf("caller(%v)-AppendEntries-rf(%v)", a[0], a[1])
			switch f.op {
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, rf.lastIndex : %v , rf.lastTerm : %v, "+
					"caller.preIndex : %v, caller.preTerm : %v, caller.entries : %v， rf.logs : %v, rf.commitId : %v\n", time, tBase+"-Success",
					a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10])
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, rf.lastIndex : %v , rf.lastTerm : %v, "+
					"caller.preIndex : %v, caller.preTerm : %v, caller.entries : %v\n", time, tBase+"-Reject", a[2], a[3], a[4], a[5], a[6], a[7], a[8])
			case Conflict:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, rf.lastIndex : %v , rf.lastTerm : %v, "+
					"caller.preIndex : %v, caller.preTerm : %v, caller.entries : %v\n", time, tBase+"-Conflict", a[2], a[3], a[4], a[5], a[6], a[7], a[8])
			case Exceed:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, rf.lastIndex : %v , rf.lastTerm : %v, "+
					"caller.preIndex : %v, caller.preTerm : %v, caller.entries : %v\n", time, tBase+"-Exceed", a[2], a[3], a[4], a[5], a[6], a[7], a[8])
			case Unexpected:
				fmt.Printf(base+"\n", time, tBase+"-Unexpected")
			}
		case sendAppendEntries:
			tBase := fmt.Sprintf("caller(%v)-sendAppendEntries-rf(%v)", a[0], a[1])
			switch f.op {
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, caller.logs : %v, caller.commitId : %v, rf.nxtInd : %v,"+
					"rf.matchInd : %v\n", time, tBase+"-Success", a[2], a[3], a[4], a[5], a[6], a[7])
			case Conflict:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, caller.logs : %v, caller.commitId : %v, rf.nxtInd : %v\n", time, tBase+"-Conflict",
					a[2], a[3], a[4], a[5], a[6])
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Rejected", a[2], a[3])
			case Lose:
				fmt.Printf(base+"\n", time, tBase+"-Lose")
			case Unexpected:
				fmt.Printf(base+"\n", time, tBase+"-Unexpected")
			}
		case Test:
			switch f.op {
			case disconnect:
				fmt.Printf(base+"\n", time, a[0])
			case connect:
				fmt.Printf(base+"\n", time, a[0])
			}
		}
	}
}

//选举的随机时间
func RandElection() int {
	return rand.Int()%150 + 300
}
