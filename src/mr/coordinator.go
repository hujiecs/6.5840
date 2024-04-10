package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "errors"
import "strconv"

type Coordinator struct {
	// Your definitions here.

	mapItems    []string // file names
	reduceItems []string // 0, 1, 2, ..., nReduce
	nReduce     int
	mapperId    int
	mapTasks    []WorkTask
	reduceTasks []WorkTask
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapItems) > 0 {
		mapItem := c.mapItems[0]
		c.mapItems = c.mapItems[1:]

		mapTask := WorkTask{taskItem: mapItem, timestamp: time.Now().Unix()}
		c.mapTasks = append(c.mapTasks, mapTask)
		reply.MapJob = &MapJob{File: mapItem, NReduce: c.nReduce, MapperId: c.mapperId}

		c.mapperId++
	} else if len(c.mapTasks) == 0 && len(c.reduceItems) > 0 {
		reduceItem := c.reduceItems[0]
		c.reduceItems = c.reduceItems[1:]

		reduceTask := WorkTask{taskItem: reduceItem, timestamp: time.Now().Unix()}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
		reply.ReduceJob = &ReduceJob{ReducerId: reduceItem}
	} else {
		reply.Done = len(c.mapItems) == 0 && len(c.reduceItems) == 0 &&
			len(c.mapTasks) == 0 && len(c.reduceTasks) == 0
	}

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.MapTask != "" {
		for i, v := range c.mapTasks {
			if v.taskItem == args.MapTask {
				c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
				return nil
			}
		}
		return errors.New("cannot find such task Id")
	} else if args.ReduceTask != "" {
		for i, v := range c.reduceTasks {
			if v.taskItem == args.ReduceTask {
				c.reduceTasks = append(c.reduceTasks[:i], c.reduceTasks[i+1:]...)
				return nil
			}
		}
		return errors.New("cannot find such task Id")
	} else {
		log.Fatal("Invalid compalte task parameter with empty map item and reduce item")
	}

	return nil
}

func (c *Coordinator) BackgroundTask() {
	for !c.Done() {
		c.mu.Lock()
		if len(c.mapTasks) > 0 {
			// loop in reverse order to avoid panic: runtime error: slice bounds out of range
			for i := len(c.mapTasks) - 1; i >= 0; i-- {
				v := c.mapTasks[i]
				if time.Now().Unix()-v.timestamp > 10 {
					c.mapItems = append(c.mapItems, v.taskItem)
					c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
				}
			}
		}

		if len(c.reduceTasks) > 0 {
			for i := len(c.reduceTasks) - 1; i >= 0; i-- {
				v := c.reduceTasks[i]
				if time.Now().Unix()-v.timestamp > 10 {
					c.reduceItems = append(c.reduceItems, v.taskItem)
					c.reduceTasks = append(c.reduceTasks[:i], c.reduceTasks[i+1:]...)
				}
			}
		}
		c.mu.Unlock()

		// sleep too long may cause timeout
		time.Sleep(time.Second)
	}
}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	ret = len(c.mapItems) == 0 && len(c.reduceItems) == 0 &&
		len(c.mapTasks) == 0 && len(c.reduceTasks) == 0

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.mapItems = files
	c.reduceItems = make([]string, nReduce)
	for i := range c.reduceItems {
		c.reduceItems[i] = strconv.Itoa(i)
	}
	c.nReduce = nReduce
	c.mapperId = 0

	go c.BackgroundTask()

	c.server()
	return &c
}
