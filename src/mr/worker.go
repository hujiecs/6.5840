package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "sort"
import "path/filepath"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			log.Fatal("Connect coordinator failed")
		}

		if reply.MapJob != nil {
			// fmt.Printf("Processing map task: %v\n", reply.MapJob.File)
			ProcessMap(mapf, reply.MapJob)
		} else if reply.ReduceJob != nil {
			// fmt.Printf("Processing reduce task: %v\n", reply.ReduceJob.ReducerId)
			ProcessReduce(reducef, reply.ReduceJob)
		} else if reply.Done {
			// fmt.Printf("All tasks completed\n")
			return
		} else {
			// fmt.Printf("Waiting for tasks...\n")
			time.Sleep(5 * time.Second)
		}

		time.Sleep(time.Second)
	}
}

func ProcessMap(mapf func(string, string) []KeyValue, job *MapJob) {
	file, err := os.Open(job.File)
	if err != nil {
		log.Fatalf("cannot open %v", job.File)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.File)
	}
	file.Close()

	kva := mapf(job.File, string(content))

	for _, kv := range kva {
		hash := ihash(kv.Key) % job.NReduce
		ofilename := fmt.Sprintf("mr-%d-%d", job.MapperId, hash)
		ofile, err := os.OpenFile(ofilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			log.Fatalf("cannot create file %v", err)
		}
		enc := json.NewEncoder(ofile)
		err = enc.Encode(kv)
		if err != nil {
			log.Fatalf("cannot encode kv %v", err)
		}
		ofile.Close()
	}

	args := CompleteTaskArgs{MapTask: job.File}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		log.Fatal("Complete map task failed")
	}
}

func ProcessReduce(reducef func(string, []string) string, job *ReduceJob) {
	ifile := fmt.Sprintf("mr-*-%v", job.ReducerId)
	matches, err := filepath.Glob(ifile)
	if err != nil {
		log.Fatalf("No such files:%v", err)
	}

	kva := []KeyValue{}
	for _, match := range matches {
		file, err := os.Open(match)
		if err != nil {
			log.Fatalf("cannot open %v", match)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	if len(kva) > 0 {
		ofilename := fmt.Sprintf("mr-out-%v", job.ReducerId)
		tempfile, err := ioutil.TempFile("", ofilename)
		if err != nil {
			log.Fatalf("cannot create temp file %v", err)
		}
		defer tempfile.Close()

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		err = os.Rename(tempfile.Name(), ofilename)
		if err != nil {
			log.Fatalf("error renaming temporary file: %v", err)
		}
	}

	args := CompleteTaskArgs{ReduceTask: job.ReducerId}
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		log.Fatal("Complete reduce task failed")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
