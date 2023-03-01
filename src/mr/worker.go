package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
func MapTask(mapf func(string, string) []KeyValue, task *Task, nReduce int) {
	content, err := os.ReadFile(task.InputFileName[0])
	if err != nil {
		log.Fatalf("read file %v error", task.InputFileName)
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
			filename := "mr-" + string(task.Number) + "-" + string(hash)
			// record while file the key in
			Key2FileMap[curKey] = filename
			IHash2KeyMap[hash] = append(IHash2KeyMap[hash], curKey)
			// close preFile
			curFile.Close()
			curFile, err = os.Create(filename)
			// resource leak will not happen
			enc = json.NewEncoder(curFile)
			if err != nil {
				log.Fatalf("failed to open the file %v", filename)
				return
			}
		}
		enc.Encode(&kv)
	}
	// send finish message to master
	call("Coordinator.FinishMap",
		&FinishMapRequest{task.Number, &Key2FileMap, &IHash2KeyMap},
		&FinishMapResponse{})
}

var reduceLock sync.Mutex

func ReduceTask(reducef func(string, []string) string, task *Task) {
	ofname := "mr-out-" + string(task.Number)

	ofile, err := os.Create(ofname)
	if err != nil {
		// todo fault-talerance
		log.Fatalf("failed to create result file [%v]", ofname)
		return
	}
	defer ofile.Close()

	// shuffle each key from intermediate file
	kvs := make(map[string][]string)
	// todo key2filemap中的key是冗余，
	for _, intermediate := range task.Key2FileMap {
		// read each intermediate file, and reduce the result
		for _, ifname := range intermediate {
			ifile, err := os.Open(ifname)
			if err != nil {
				log.Fatalf("failed to open the intermediate file [%v]", ifname)
				return
			}
			dec := json.NewDecoder(ifile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
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
		task, nReduce := GetTask()
		if task != nil {
			switch task.Type {
			case MapTaskType:
				MapTask(mapf, task, nReduce)
			case ReduceTaskType:
				ReduceTask(reducef, task)
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
func GetTask() (*Task, int) {
	req := GetTaskRequest{}
	resp := GetTaskResponse{}

	ok := call("Coordinator.GetTask", &req, &resp)
	if !ok {
		fmt.Printf("get Task failed,the reason is: %v", ok)
		return nil, 0
	}
	return &resp.Task, resp.NReduce
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
