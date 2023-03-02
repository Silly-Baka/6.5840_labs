package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Offer your RPC definitions here.

type GetTaskRequest struct {
}

type GetTaskResponse struct {
	Task       *Task
	NReduce    int
	IsFinished bool
}

type FinishMapRequest struct {
	TaskNum      int
	Key2FileMap  *map[string]string
	IHash2KeyMap *map[int][]string
}

type FinishMapResponse struct {
}

type FinishReduceRequest struct {
	TaskNum int
}

type FinishReduceResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
