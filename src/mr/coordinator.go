package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	TaskNum int
	// the most num of reduce task
	NReduce int

	//// list of map workers ，can be changed to ip address , now use socketname
	//// todo 数量应该不用考虑？每有一个rpc连接就增加一个
	//MapWorkers []string
	//// list of reduce workers
	//ReduceWorkers []string
	//// word to worker's socketName
	//WorkerMap map[string]string

	TaskList          []Task
	FreeTaskQueue     Queue
	FinishedTaskQueue Queue
}
type Task struct {
	Number        int
	InputFileName string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	// todo 这里的pop操作要考虑加锁
	data := c.FreeTaskQueue.Pop()
	if data == nil {
		return fmt.Errorf("there is no free task")
	}
	task, ok := data.(Task)
	if !ok {
		return fmt.Errorf("task %v error", data)
	}
	// 发出任务后，启动一个10s的计时器，到期就将该任务重新放回队列，便于获取
	time.AfterFunc(10*time.Second, func() {
		c.FreeTaskQueue.Offer(task)
	})

	resp.task = task
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	// todo 这里的handler需要自己完善一下，用于处理rpc通信的逻辑，保存handler的信息
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	l := len(files)
	c := Coordinator{
		TaskNum:           l,
		NReduce:           nReduce,
		TaskList:          make([]Task, l),
		FreeTaskQueue:     Queue{},
		FinishedTaskQueue: Queue{},
	}
	// Your code here.
	for n, file := range files {
		task := Task{
			Number:        n,
			InputFileName: file,
		}
		c.TaskList[n] = task
		c.FreeTaskQueue.Offer(task)
	}

	c.server()
	return &c
}
