package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type StatusType int

const (
	NOALLOCATE_STATUS StatusType = iota
	WAITING_STATUS
	FINISHED_STATUS
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int          // number of reduce
	nMap           int          // number of map
	files          []string     // input filename
	mapFinished    int          // number of finished map
	mapStatus      []StatusType // status of map task
	reduceFinished int          // number of finished reduce
	reduceStatus   []StatusType // status of reduce task
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapStatus[args.TaskID] == WAITING_STATUS {
		c.mapFinished++
		c.mapStatus[args.TaskID] = FINISHED_STATUS
	}
	return nil
}

func (c *Coordinator) ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceStatus[args.TaskID] == WAITING_STATUS {
		c.reduceFinished++
		c.reduceStatus[args.TaskID] = FINISHED_STATUS
	}
	return nil
}

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	allocateTask := WAITING_TASK
	allocateID := -1
	if c.mapFinished < c.nMap {
		allocateTask = MAP_TASK
		for i := 0; i < c.nMap; i++ {
			if c.mapStatus[i] == NOALLOCATE_STATUS {
				allocateID = i
				c.mapStatus[allocateID] = WAITING_STATUS
				// if ten seconds can`t finish the map task, judge the worker died
				go func(id int) {
					time.Sleep(time.Duration(10) * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.mapStatus[id] == WAITING_STATUS {
						c.mapStatus[id] = NOALLOCATE_STATUS
					}
				}(allocateID)
				break
			}
		}
	} else if c.reduceFinished < c.nReduce {
		allocateTask = REDUCE_TASK
		for i := 0; i < c.nReduce; i++ {
			if c.reduceStatus[i] == NOALLOCATE_STATUS {
				allocateID = i
				c.reduceStatus[allocateID] = WAITING_STATUS
				// if ten seconds can`t finish the reduce task, judge the worker died
				go func(id int) {
					time.Sleep(time.Duration(10) * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.reduceStatus[id] == WAITING_STATUS {
						c.reduceStatus[id] = NOALLOCATE_STATUS
					}
				}(allocateID)
				break
			}
		}
	} else {
		reply.TaskType = FINISHED_TASK
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if allocateID == -1 {
		reply.TaskType = WAITING_TASK
	} else {
		reply.TaskType = allocateTask
		reply.TaskID = allocateID
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		if allocateTask == MAP_TASK {
			reply.FileName = c.files[allocateID]
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := c.reduceFinished == c.nReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapStatus = make([]StatusType, c.nMap)
	c.reduceStatus = make([]StatusType, c.nReduce)
	c.server()
	return &c
}
