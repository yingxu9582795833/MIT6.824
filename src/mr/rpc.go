package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
)
const (
	MapPhase Phase = iota
	ReducePhase
)
const (
	Idle State = iota
	Running
	Finished
)

type TaskType int //任务类型
type Phase int    //阶段
type Task struct {
	FileNames []string
	TaskType  TaskType
	ReduceNum int
	TaskId    int
}
type State int
type TaskInfo struct {
	TaskState State
	StartTime time.Time
	Task      Task
}
type R bool

type DoneArgs struct {
	TempFiles []string
	Type      TaskType
	TaskId    int
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RequestArgs struct {
}

type ResponseInfo struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
