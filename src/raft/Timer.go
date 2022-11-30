package raft

import (
	"sync"
	"time"
)

type Timer struct {
	waitTime int
	event    Event
	raft     *Raft
	timer    *time.Timer
	mu       sync.Mutex
}

/**
Timer
makeTimer returns a newly started timer.
*/
func makeTimer(waitTime int, event Event, rf *Raft) *Timer {
	timer := &Timer{
		waitTime: waitTime,
		event:    event,
		raft:     rf,
	}
	timer.timer = time.NewTimer(10000 * time.Second)
	go func(timer *Timer) {
		timer.stop()
		for {
			<-timer.timer.C
			timer.raft.eventCh <- event
			timer.start()
		}
	}(timer)
	return timer
}

func (timer *Timer) setWaitTime(waitMs int) {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	timer.waitTime = waitMs
}

func (timer *Timer) stop() {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if !timer.timer.Stop() {
		select {
		case <-timer.timer.C:
		default:
		}
	}
}

func (timer *Timer) start() {
	timer.stop()
	timer.mu.Lock()
	duration := time.Duration(timer.waitTime) * time.Millisecond
	defer timer.mu.Unlock()
	timer.timer.Reset(duration)
}

func (timer *Timer) clear() {
	timer.start()
}
