package raft

import (
	"sync"
	"time"
)

type Timer struct {
	waitTime int
	doAction func()
	raft     *Raft
	timer    *time.Timer
	mu       sync.Mutex
}

/**
Timer
makeTimer returns a newly started timer.
*/
func makeTimer(waitTime int, doAction func(), rf *Raft) *Timer {
	timer := &Timer{
		waitTime: waitTime,
		doAction: doAction,
		raft:     rf,
	}
	timer.timer = time.NewTimer(3600 * time.Second)
	go func(timer *Timer) {
		//注意这个地方不能有timer.stop
		//因为Make里面的定时器如果先reset完毕，这里可能会破坏掉Reset的操作
		//timer.stop()
		for {
			<-timer.timer.C
			go doAction() //
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
