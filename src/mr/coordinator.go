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

	IHash2KeyMap map[int][]string
	Key2FileMap  map[string][]string
}
type Task struct {
	Number        int
	Type          int // 0-Map, 1-Reduce
	InputFileName string
	Key2FileMap   map[string][]string
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
		// todo 这里的pop操作要考虑加锁
		data := c.FreeMapTaskQueue.Pop()
		if data == nil {
			return fmt.Errorf("there is no free Task")
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
	} else if len(c.FinishedReduceTask) < c.NReduce {
		// return reduce Task

		// initialize reduce Task, only once
		InitReduce.Do(func() {
			for reduceNum, keys := range c.IHash2KeyMap {
				km := make(map[string][]string)
				for _, key := range keys {
					if km[key] == nil {
						km[key] = make([]string, 0)
					}
					km[key] = append(km[key], c.Key2FileMap[key]...)
				}
				newTask := Task{
					Number:      reduceNum,
					Type:        ReduceTaskType,
					Key2FileMap: km,
				}
				c.FreeReduceTaskQueue.Offer(newTask)
			}
		})
		// get a free reduce Task and return
		data := c.FreeReduceTaskQueue.Pop()
		if data == nil {
			return fmt.Errorf("there is no free Task")
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
	c.FinishedMapTask[req.TaskNum] = true
	// add key-filename into map
	for k, v := range *req.Key2FileMap {
		// todo 需要考虑线程安全
		if c.Key2FileMap[k] == nil {
			if lock.TryLock() {
				if c.Key2FileMap[k] == nil {
					c.Key2FileMap[k] = make([]string, 0)
				}
				lock.Unlock()
			}
		}
		c.Key2FileMap[k] = append(c.Key2FileMap[k], v)
	}
	// add hash-key into map
	for hash, keys := range *req.IHash2KeyMap {
		if c.IHash2KeyMap[hash] == nil {
			if lock.TryLock() {
				if c.IHash2KeyMap[hash] == nil {
					c.IHash2KeyMap[hash] = make([]string, 0)
				}
				lock.Unlock()
			}
		}
		c.IHash2KeyMap[hash] = append(c.IHash2KeyMap[hash], keys...)
	}
	return nil
}

func (c *Coordinator) FinishReduce(req *FinishReduceRequest, resp *FinishReduceResponse) error {
	c.FinishedReduceTask[req.TaskNum] = true
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

	// close coordinator when all task were finished
	if len(c.FinishedMapTask) == c.NMap && len(c.FinishedReduceTask) == c.NReduce {
		log.Fatal("all tasks are finished, coordinator will be closed")
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	fmt.Printf("inilitializing coordinator, the files are : %v", files)
	l := len(files)
	c := Coordinator{
		NMap:                l,
		NReduce:             nReduce,
		FreeMapTaskQueue:    Queue{},
		FreeReduceTaskQueue: Queue{},
		FinishedMapTask:     make(map[int]bool),
		FinishedReduceTask:  make(map[int]bool),
	}
	// Your code here.
	for n, file := range files {
		task := Task{
			Number:        n,
			InputFileName: file,
		}
		c.FreeMapTaskQueue.Offer(task)
	}

	c.server()
	return &c
}
