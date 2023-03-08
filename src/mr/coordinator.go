package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	NMap int
	// the most num of reduce Task
	NReduce int

	//// list of map workers ，can be changed to ip address , now use socketname
	//// todo 数量应该不用考虑？每有一个rpc连接就增加一个
	//MapWorkers []string
	//// list of reduce workers
	//ReduceWorkers []string
	//// word to worker's socketName
	//WorkerMap map[string]string
	FreeMapTaskQueue    Queue
	FreeReduceTaskQueue Queue
	FinishedMapTask     map[int]bool
	FinishedReduceTask  map[int]bool

	Hash2FileMap map[int][]string
}
type Task struct {
	Number        int
	Type          int // 0-Map, 1-Reduce
	InputFileName []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

var InitReduce sync.Once

// get map Task and reduce Task ( when all map tasks have been finished  )
func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	// there are still unfinished map tasks
	if len(c.FinishedMapTask) < c.NMap {
		// return map Task
		data := c.FreeMapTaskQueue.Pop()
		if data == nil {
			//return fmt.Errorf("there is no free Task")
			return nil
		}
		task, ok := data.(Task)
		if !ok {
			return fmt.Errorf("Task %v error", data)
		}
		// 发出任务后，启动一个10s的计时器，到期就将该任务重新放回队列，便于获取
		time.AfterFunc(10*time.Second, func() {
			if !c.FinishedMapTask[task.Number] {
				c.FreeMapTaskQueue.Offer(task)
			}
		})
		resp.Task = &task
		resp.IsFinished = false
	} else if len(c.FinishedReduceTask) < c.NReduce {
		// return reduce Task
		//fmt.Println("initializing reduce tasks")

		// initialize reduce Task, only once
		InitReduce.Do(func() {
			for reduceNum, fs := range c.Hash2FileMap {
				newTask := Task{
					Number:        reduceNum,
					Type:          ReduceTaskType,
					InputFileName: fs,
				}
				c.FreeReduceTaskQueue.Offer(newTask)
			}
		})
		// get a free reduce Task and return
		data := c.FreeReduceTaskQueue.Pop()
		if data == nil {
			//return fmt.Errorf("there is no free Task")
			return nil
		}
		task, ok := data.(Task)
		if !ok {
			return fmt.Errorf("Task %v error", data)
		}
		// 发出任务后，启动一个10s的计时器，到期就将该任务重新放回队列，便于获取
		time.AfterFunc(10*time.Second, func() {
			if !c.FinishedReduceTask[task.Number] {
				c.FreeReduceTaskQueue.Offer(task)
			}
		})
		resp.Task = &task
		resp.IsFinished = false

	} else {
		// all tasks are finished
		resp.IsFinished = true
	}
	resp.NReduce = c.NReduce

	return nil
}

var lock sync.Mutex

func (c *Coordinator) FinishMap(req *FinishMapRequest, resp *FinishMapResponse) error {
	// record Finished Task
	lock.Lock()
	defer lock.Unlock()
	c.FinishedMapTask[req.TaskNum] = true

	// add hash-filename into map
	for hash, files := range *req.Hash2FileMap {
		if c.Hash2FileMap[hash] == nil {
			//lock.Lock()
			//if c.Hash2FileMap[hash] == nil {
			//	c.Hash2FileMap[hash] = make([]string, 0)
			//}
			//lock.Unlock()
			c.Hash2FileMap[hash] = make([]string, 0)
		}
		//lock.Lock()
		c.Hash2FileMap[hash] = append(c.Hash2FileMap[hash], files...)
		//lock.Unlock()
	}

	return nil
}

func (c *Coordinator) FinishReduce(req *FinishReduceRequest, resp *FinishReduceResponse) error {

	lock.Lock()
	defer lock.Unlock()
	c.FinishedReduceTask[req.TaskNum] = true

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":8888")
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
	lock.Lock()
	defer lock.Unlock()

	return len(c.FinishedMapTask) == c.NMap && len(c.FinishedReduceTask) == c.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Printf("inilitializing coordinator, the files are : %v", files)
	l := len(files)
	c := Coordinator{
		NMap:                l,
		NReduce:             nReduce,
		FreeMapTaskQueue:    Queue{},
		FreeReduceTaskQueue: Queue{},
		FinishedMapTask:     make(map[int]bool),
		FinishedReduceTask:  make(map[int]bool),
		Hash2FileMap:        make(map[int][]string),
	}
	// Your code here.
	for n, file := range files {
		task := Task{
			Number:        n,
			Type:          MapTaskType,
			InputFileName: []string{file},
		}
		c.FreeMapTaskQueue.Offer(task)
	}

	c.server()
	return &c
}
