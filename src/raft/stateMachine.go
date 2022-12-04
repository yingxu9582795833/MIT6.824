package raft

import (
	"fmt"
	"sync"
)

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
	getName() string
}

const (
	electionTimeOutIndex int = iota
	heartTimeOutIndex
	higherTermIndex
	receiveMajorityIndex
)

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
func (event *electionFailure) getName() string {
	return "electionFailure"
}

type electionTimeOut struct {
	Index int
}

func (event *electionTimeOut) getId() int {
	return event.Index
}
func (event *electionTimeOut) getName() string {
	return "electionTimeOut"
}

type heartTimeOut struct {
	Index int
}

func (event *heartTimeOut) getId() int {
	return event.Index
}
func (event *heartTimeOut) getName() string {
	return "heartTimeOut"
}

type Action int

const (
	appendEntriesRejected Action = iota
	electionRejected
	appendEntriesSucceed
	electionSucceed
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
func (event *higherTerm) getName() string {
	return "higherTerm"
}

type receiveMajority struct {
	Index int
}

func (event *receiveMajority) getId() int {
	return event.Index
}
func (event *receiveMajority) getName() string {
	return "receiveMajority"
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
	notTransfer
	ErrorState
)

type tranIndex struct {
	smState SMState
	eventId int
}
type tran interface {
	transfer(args ...interface{}) SMState
}

type notChangeAnything struct {
	raft *Raft
}

func (tran *notChangeAnything) transfer(args ...interface{}) SMState {
	tran.raft.StateMachine.rwmu.RLock()
	defer tran.raft.StateMachine.rwmu.RUnlock()
	return tran.raft.StateMachine.smState
}
func (stateMachine *StateMachine) initTranTable() {
	stateMachine.tranTable = make(map[tranIndex]tran)
	stateMachine.tranTable[tranIndex{smState: Leader, eventId: heartTimeOutIndex}] = &heartTicker{raft: stateMachine.raft}
	stateMachine.tranTable[tranIndex{smState: Leader, eventId: higherTermIndex}] = &toBeFollower{raft: stateMachine.raft}
	stateMachine.tranTable[tranIndex{smState: Candidate, eventId: electionTimeOutIndex}] = &startElection{raft: stateMachine.raft}
	stateMachine.tranTable[tranIndex{smState: Candidate, eventId: higherTermIndex}] = &toBeFollower{raft: stateMachine.raft}
	stateMachine.tranTable[tranIndex{smState: Candidate, eventId: receiveMajorityIndex}] = &toBeLeader{raft: stateMachine.raft}
	stateMachine.tranTable[tranIndex{smState: Follower, eventId: electionTimeOutIndex}] = &startElection{raft: stateMachine.raft}
	stateMachine.tranTable[tranIndex{smState: Follower, eventId: higherTermIndex}] = &toBeFollower{raft: stateMachine.raft}
}
func (stateMachine *StateMachine) mainLoop() {
	for {
		//fmt.Println("test come here")
		event := <-stateMachine.eventCh
		DPrintf(Func{fType: SM, op: eventCome}, stateMachine.raft.me, event.getName())
		//【highterm】重置了时钟，但是在它重置之前，election.timer已经触发了事件发送了时钟并开始选举
		stateMachine.rwmu.Lock()
		curState := stateMachine.smState
		fmt.Printf("%v,%v,%v\n", event.getId(), curState, stateMachine.tranTable[tranIndex{eventId: event.getId(), smState: curState}])
		tr := stateMachine.tranTable[tranIndex{eventId: event.getId(), smState: curState}]
		if tr == nil {
			continue
		}
		dist := stateMachine.tranTable[tranIndex{eventId: event.getId(), smState: curState}].transfer(event)
		if dist != notTransfer {
			stateMachine.smState = dist
		}
		stateMachine.rwmu.Unlock()
	}
}
