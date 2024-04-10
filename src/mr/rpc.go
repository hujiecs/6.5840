package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.

type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool
}

type MapJob struct {
	// returns by coordinator, cannot use global counter in worker
	// because each worker runs in different process
	MapperId int
	File     string
	NReduce  int
}

type ReduceJob struct {
	ReducerId string
}

type WorkTask struct {
	timestamp int64
	taskItem  string
}

type CompleteTaskArgs struct {
	MapTask    string
	ReduceTask string
}

type CompleteTaskReply struct {
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
