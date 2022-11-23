package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// mutex for push tasks
var (
	mu sync.Mutex
)
var tid int

type Coordinator struct {
	// Your definitions here.
	MapTasks      []Task
	ReduceTasks   []Task
	PhaseChannel  chan bool
	DoneChannel   chan bool
	CurrentPhase  Phase
	ReduceNum     int
	MapNum        int
	finishMap     int
	finishReuduce int
	TempFiles     [][]string
	TaskInfos     []TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = <-c.DoneChannel
	fmt.Printf("\ncoordinator 退出程序\n")
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		CurrentPhase: MapPhase,
		ReduceNum:    nReduce,
		MapNum:       len(files),
		PhaseChannel: make(chan bool),
		TempFiles:    make([][]string, nReduce),
		DoneChannel:  make(chan bool),
		TaskInfos:    make([]TaskInfo, len(files)+nReduce),
	}

	// Your code here.
	//1.先制作一批map任务，等到worker节点的拉取
	ok := c.makeMapTasks(files)
	go c.crashDetector()
	c.server()
	if !ok {
		log.Fatal("map任务创建失败")
	}
	//2.map任务完成后，需要转换到reduce任务阶段
	ok = c.phaseToReduce()
	if !ok {
		log.Fatal("无法进行阶段转换")
	}
	ok = c.makeReduceTasks(nReduce)
	//3.reduce任务阶段，等待workder节点的拉取

	return &c
}

//CallDone 标记完成任务，并且向Coordinator提供一些信息
func (c *Coordinator) CallDone(args *DoneArgs, response *R) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.Type {
	case MapTask:
		c.finishMap++
		c.saveMapProduceFile(args)
	case ReduceTask:
		c.finishReuduce++
		c.saveReduceProduceFile(args)
	}
	//看是否完成了所有map任务和所有reduce任务
	//fmt.Println("\nMap任务的数量", c.finishMap)
	//fmt.Println("Reduce任务的数量", c.finishReuduce)
	c.setTaskFinished(args.TaskId)
	//要注意一切可能发生堵塞的地方
	if c.finishMap == c.MapNum && c.finishReuduce == 0 {
		c.PhaseChannel <- true
	}
	if c.finishReuduce == c.ReduceNum {
		c.DoneChannel <- true
	}
	*response = true
	return nil
}
func (c *Coordinator) crashDetector() {
	for {
		time.Sleep(time.Second * 2)
		c.crashDetect()
	}
}
func (c *Coordinator) crashDetect() {
	mu.Lock()
	defer mu.Unlock()
	for i, taskInfo := range c.TaskInfos {
		if taskInfo.TaskState == Running && time.Since(taskInfo.StartTime) > time.Second*9 {
			//fmt.Printf("哪个任务没完成%v\n", taskInfo)
			if taskInfo.Task.TaskType == MapTask {
				c.MapTasks = append(c.MapTasks, taskInfo.Task)
			} else {
				c.ReduceTasks = append(c.ReduceTasks, taskInfo.Task)
			}
			c.TaskInfos[i].TaskState = Idle
			c.TaskInfos[i].StartTime = time.Now()
		}
	}
}
func (c *Coordinator) saveMapProduceFile(args *DoneArgs) {
	files := args.TempFiles
	//fmt.Printf("保存的临时文件: %v", files)
	for i, file := range files {
		c.TempFiles[i] = append(c.TempFiles[i], file)
	}
}

func (c *Coordinator) saveReduceProduceFile(args *DoneArgs) {

}

// PushTask 分发任务
func (c *Coordinator) PushTask(args *RequestArgs, response *Task) error {
	//1.首先得上锁
	mu.Lock()
	defer mu.Unlock()
	//2.然后开始分发
	//fmt.Printf("\n开始分发任务\n")
	//判断现在属于哪个阶段
	switch c.CurrentPhase {
	case MapPhase:
		if len(c.MapTasks) > 0 { //可以拉取
			task := c.MapTasks[0]
			//fmt.Printf("获得任务 %v\n", task)
			ok := c.setTaskRunning(task.TaskId)
			if !ok {
				panic("拉取失败，任务正在工作\n")
			}
			*response = task
			c.MapTasks = c.MapTasks[1:]
		} else { //任务都分发完了，告诉worker现在正在等待
			response.TaskType = WaitingTask
		}
	case ReducePhase:
		if len(c.ReduceTasks) > 0 {
			task := c.ReduceTasks[0]
			ok := c.setTaskRunning(task.TaskId)
			if !ok {
				panic("拉取失败，任务正在工作\n")
			}
			*response = task
			c.ReduceTasks = c.ReduceTasks[1:]
		} else { //任务都发完了，但是可能在执行
			response.TaskType = WaitingTask
		}
	}
	return nil
}
func (c *Coordinator) setTaskRunning(taskId int) bool {
	if c.TaskInfos[taskId].TaskState == Running {
		return false
	} else {
		c.TaskInfos[taskId].TaskState = Running
		c.TaskInfos[taskId].StartTime = time.Now()
		return true
	}
}
func (c *Coordinator) setTaskFinished(taskId int) bool {
	if c.TaskInfos[taskId].TaskState == Idle {
		return false
	} else {
		c.TaskInfos[taskId].TaskState = Finished
		return true
	}
}
func (c *Coordinator) makeMapTasks(files []string) bool {
	//需要把任务存在Coordinator中，方便拉取
	for _, fileName := range files {
		task := Task{
			FileNames: []string{fileName},
			TaskType:  MapTask,
			TaskId:    c.genTaskId(),
			ReduceNum: c.ReduceNum,
		}
		c.TaskInfos[task.TaskId] = TaskInfo{
			Task:      task,
			TaskState: Idle,
		}
		c.MapTasks = append(c.MapTasks, task)
	}
	return true
}

//读写一个共享数据要么加锁，要么将它放在管道里让人读取
func (c *Coordinator) makeReduceTasks(ReduceNum int) bool {
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < ReduceNum; i++ {
		task := Task{
			FileNames: c.TempFiles[i],
			TaskType:  ReduceTask,
			TaskId:    c.genTaskId(),
			ReduceNum: c.ReduceNum,
		}
		c.TaskInfos[task.TaskId] = TaskInfo{
			Task:      task,
			TaskState: Idle,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}
	log.Println("\nReduce任务制作完毕")
	return true
}
func (c *Coordinator) genTaskId() int {
	defer func() { tid++ }()
	return tid
}
func (c *Coordinator) phaseToReduce() bool {
	//map任务全部做完之后，通知phaseToReduce进行阶段转换工作
	//通过channel通知，
	ok := <-c.PhaseChannel
	c.CurrentPhase = ReducePhase
	return ok
}
