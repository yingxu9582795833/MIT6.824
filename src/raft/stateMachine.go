package raft

import "sync"

type StateMachine struct {
	smState   SMState
	event     Event
	tranTable map[tranIndex]tran
	raft      *Raft
	rwmu      sync.RWMutex
	eventCh   chan Event
}
type SMState int
type Event interface {
	getId() int
}

const (
	electionTimeOutIndex int = iota
	heartTimeOutIndex
	higherTermIndex
	receiveMajorityIndex
)

type timeOut struct {
	Index int
}

func (event *timeOut) getId() int {
	return event.Index
}

type electionSuccess struct {
	Index int
}

func (event *electionSuccess) getId() int {
	return event.Index
}

type electionFailure struct {
	Index int
}

func (event *electionFailure) getId() int {
	return event.Index
}

type electionTimeOut struct {
	Index int
}

func (event *electionTimeOut) getId() int {
	return event.Index
}

type heartTimeOut struct {
	Index int
}

func (event *heartTimeOut) getId() int {
	return event.Index
}

type Action int

const (
	appendEntriesRejected Action = iota
	electionRejected
)

type higherTerm struct {
	Index     int
	highTerm  int    //对方的任期
	highIndex int    //对方的索引
	action    Action //子动作
}

func (event *higherTerm) getId() int {
	return event.Index
}

type receiveMajority struct {
	Index int
}

func (event *receiveMajority) getId() int {
	return event.Index
}

//const (
//	timeOut Event = iota
//	electionSuccess
//	electionFailure
//	heartTick
//	higherTerm
//)
const (
	Follower SMState = iota
	Candidate
	Leader
	ErrorState
)

type tranIndex struct {
	smState SMState
	event   Event
}
type tran interface {
	transfer(args ...interface{}) SMState
}

func (stateMachine *StateMachine) mainLoop() {
	for {
		event := <-stateMachine.eventCh
		stateMachine.rwmu.Lock()
		curState := stateMachine.smState
		dist := stateMachine.tranTable[tranIndex{event: event, smState: curState}].transfer()
		stateMachine.smState = dist
		stateMachine.rwmu.Unlock()
	}
}
