package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// actual logic of the map Task
func DoMap(mapf func(string, string) []KeyValue, task *Task, nReduce int) {

	fmt.Printf("doing map task : %v \n", task)
	content, err := os.ReadFile(task.InputFileName)
	if err != nil {
		fmt.Printf("read file %s error\n", task.InputFileName)
		return
	}
	kva := mapf("", string(content))
	intermediate := []KeyValue{}

	intermediate = append(intermediate, kva...)

	// sort by key and divide into different buckets
	sort.Sort(ByKey(intermediate))

	curKey := ""
	var curFile *os.File
	var enc *json.Encoder

	Key2FileMap := make(map[string]string)
	IHash2KeyMap := make(map[int][]string)
	for _, kv := range intermediate {
		if kv.Key != curKey {
			// create new intermediate file and output result
			curKey = kv.Key
			hash := ihash(curKey) % nReduce
			filename := "mr-" + strconv.Itoa(task.Number) + "-" + strconv.Itoa(hash)
			// record while file the key in
			Key2FileMap[curKey] = filename
			if IHash2KeyMap[hash] == nil {
				// todo
				IHash2KeyMap[hash] = make([]string, 0)
			}
			IHash2KeyMap[hash] = append(IHash2KeyMap[hash], curKey)
			// close preFile
			curFile.Close()
			curFile, err = os.Create(filename)
			// resource leak will not happen
			if err != nil {
				fmt.Printf("failed to open the file %v\n", filename)
				return
			}
			enc = json.NewEncoder(curFile)
		}
		enc.Encode(&kv)
	}
	// send finish message to master
	ok := call("Coordinator.FinishMap",
		&FinishMapRequest{task.Number, &Key2FileMap, &IHash2KeyMap},
		&FinishMapResponse{})
	if !ok {
		fmt.Println("failed to finish map")
		return
	}
}

var reduceLock sync.Mutex

func DoReduce(reducef func(string, []string) string, task *Task) {

	log.Printf("doing reduce task : %v\n ", task)
	ofname := "mr-out-" + strconv.Itoa(task.Number)

	ofile, err := os.Create(ofname)
	if err != nil {
		// todo fault-talerance
		fmt.Printf("failed to create result file [%v]\n", ofname)
		return
	}
	defer ofile.Close()

	// shuffle each key from intermediate file
	kvs := make(map[string][]string)

	fileFlag := make(map[string]bool)

	// todo key2filemap中的key是冗余，
	for _, intermediate := range task.Key2FileMap {
		// read each intermediate file, and reduce the result
		for _, ifname := range intermediate {
			if fileFlag[ifname] {
				continue
			}
			ifile, err := os.Open(ifname)
			// record the checked file
			fileFlag[ifname] = true

			if err != nil {
				fmt.Printf("failed to open the intermediate file [%v]\n", ifname)
				return
			}
			dec := json.NewDecoder(ifile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err == nil {
					key := kv.Key
					vs := kvs[key]
					if vs == nil {
						if reduceLock.TryLock() {
							vs = kvs[key]
							if vs == nil {
								vs = make([]string, 0)
							}
							reduceLock.Unlock()
						}
					}
					vs = append(vs, kv.Value)
					kvs[key] = vs
				} else {
					break
				}
			}
			ifile.Close()
		}
	}
	// reduce output
	for k, vs := range kvs {
		output := reducef(k, vs)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	// send finish message to master
	ok := call("Coordinator.FinishReduce", &FinishReduceRequest{TaskNum: task.Number}, &FinishReduceResponse{})
	if !ok {
		log.Fatal("failed to finish reduce")
	}

	// this is the correct format for each line of Reduce output.
	//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task, nReduce, isFinished := GetTask()

		// job finished, close this worker
		if isFinished {
			fmt.Println("all job finished, worker closing")
			return
		}

		fmt.Printf("the task infomation is : %v\n", task)
		if task != nil {
			switch task.Type {
			case MapTaskType:
				DoMap(mapf, task, nReduce)
			case ReduceTaskType:
				DoReduce(reducef, task)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// return Task and NReduce
func GetTask() (*Task, int, bool) {
	var req GetTaskRequest
	var resp GetTaskResponse

	ok := call("Coordinator.GetTask", &req, &resp)
	if !ok {
		fmt.Printf("get Task failed,the reason is: %v", ok)
		return nil, 0, false
	}
	return resp.Task, resp.NReduce, resp.IsFinished
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":8888")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
