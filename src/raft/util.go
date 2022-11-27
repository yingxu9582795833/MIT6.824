package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//选举的随机时间
func RandElection() time.Duration {
	return time.Duration(rand.Int()%250+150) * time.Millisecond
}
