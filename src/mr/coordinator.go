package mr

import (
	"fmt"
	"log"
	"sync"
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
	PhaseChannel  chan bool
	CurrentPhase  Phase
	ReduceNum     int
	MapNum        int
	finishMap     int
	finishReuduce int
	TempFiles     [][]string
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

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{

	}

	c.server()
	// Your code here.
	//1.先制作一批map任务，等到worker节点的拉取
	fmt.Println("看一下能不能到这一步")
	ok := c.makeMapTasks(files)
	fmt.Printf("Coordinator : %v", c)
	if !ok {
		log.Fatal("map任务创建失败")
	}
	//2.map任务完成后，需要转换到reduce任务阶段
	ok = c.phaseToReduce()
	if !ok {
		log.Fatal("无法进行阶段转换")
	}
	//	每个map任务完成后，需要收到worker节点的通知
	//  所有map任务完成后，需要转换到reduce任务阶段，要制作一批reduce任务等待拉取
	//makeReduceTasks(nReduce)
	//3.reduce任务阶段，等待workder节点的拉取

	return &c
}

//CallDone 标记完成任务，并且向Coordinator提供一些信息
func (c *Coordinator) CallDone(args *DoneArgs, response *R) {
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
	if c.finishMap == c.MapNum {
		c.PhaseChannel <- true
	}
	if c.finishReuduce == c.ReduceNum {

	}
	*response = true
}

func (c *Coordinator) saveMapProduceFile(args *DoneArgs) {
	files := args.TempFiles
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
	//判断现在属于哪个阶段
	fmt.Printf("开始分发任务\n")
	switch c.CurrentPhase {
	case MapPhase:
		if len(c.MapTasks) > 0 { //可以拉取
			task := c.MapTasks[0]
			fmt.Printf("分发任务: %v\n", task)
			c.MapTasks = c.MapTasks[1:]
			*response = task
		} else { //任务都分发完了，接下来看是否完成了所有任务

		}
	case ReducePhase:

	}
	return nil
}
func (c *Coordinator) makeMapTasks(files []string) bool {
	//需要把任务存在Coordinator中，方便拉取
	for _, fileName := range files {
		task := Task{
			fileNames: []string{fileName},
			TaskType:  MapTask,
			TaskId:    c.genTaskId(),
			ReduceNum: c.ReduceNum,
		}
		c.MapTasks = append(c.MapTasks, task)
	}
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
	return ok
}
