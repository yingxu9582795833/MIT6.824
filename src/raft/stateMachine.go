package raft

type StateMachine struct {
	smState   SMState
	event     Event
	tranTable map[tranIndex]tran
	raft      *Raft
}
type SMState int
type Event int

const (
	Follower SMState = iota
	Candidate
	Leader
)

type tranIndex struct {
	smState SMState
	event   Event
}
type tran interface {
	transfer() SMState
}
