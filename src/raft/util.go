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
const Debug = false

type FType int

const (
	RequestVote FType = iota
	sendRequestVote
	AppendEntries
	sendAppendEntries
	heartTick
	electionTick
	Test
)

type Op int

const (
	Rejected Op = iota
	Success
	Start
	BeLeader
	Used
	Lose
)

type Func struct {
	fType FType
	op    Op
}

func DPrintf(f Func, a ...interface{}) {
	base := "[%-8v  %-40s]: "
	time := time.Since(debugStart).Microseconds()
	time /= 100
	if Debug {
		switch f.fType {
		case RequestVote:
			tBase := fmt.Sprintf("caller(%v)-RequestVote-rf(%v)", a[0], a[1])
			switch f.op {
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Reject", a[2], a[3])
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Success", a[2], a[3])
			}
		case sendRequestVote:
		case electionTick:
			tBase := fmt.Sprintf("caller(%v)-electionTick-rf(%v)", a[0], a[1])
			switch f.op {
			case Start:
				fmt.Printf(base+"caller.term: %v\n", time, tBase+"-Start", a[2])
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Reject", a[2], a[3])
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, voted-num: %v\n", time, tBase+"-Success", a[2], a[3], a[4])
			case BeLeader:
				fmt.Printf(base+"rf.term : %v, caller.term : %v, voted-num: %v, peer nums : %v\n", time, tBase+"-BeLeader", a[2], a[3], a[4], a[5])
			case Used:
				fmt.Printf(base+"\n", time, tBase+"-Used")
			case Lose:
				fmt.Printf(base+"\n", time, tBase+"-Lose")
			}
		case AppendEntries:
			tBase := fmt.Sprintf("caller(%v)-AppendEntries-rf(%v)", a[0], a[1])
			switch f.op {
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Success", a[2], a[3])
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Rejected", a[2], a[3])
			}
		case heartTick:
			tBase := fmt.Sprintf("caller(%v)-heartTick-rf(%v)", a[0], a[1])
			switch f.op {
			case Success:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Success", a[2], a[3])
			case Rejected:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Rejected", a[2], a[3])
			case Lose:
				fmt.Printf(base+"rf.term : %v, caller.term : %v\n", time, tBase+"-Lose", a[2], a[3])
			}
		case Test:
			fmt.Printf(base+"caller: %v\n", time, "", a[0])
		}
	}
}

//RandElection 选举的随机时间
func RandElection() int {
	return rand.Int()%150 + 300
}
