package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	nReduce        int      // number of reduce
	nMap           int      // number of map
	files          []string // input filename
	mapFinished    int      // number of finished map
	mapStatus      []int    // status of map task
	reduceFinished int      // number of finished reduce
	reduceStatus   []int    // status of reduce task
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AllocateTask(args *WorkerArgs, reply *WorkerReply) (err error) {
	if c.mapFinished < c.nMap {
		allocateID := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapStatus[i] == 0 {
				allocateID = i
				break
			}
		}
		if allocateID == -1 {
			// waiting for map job finished
			reply.TaskType = TASK_WAITING
		} else {
			reply.TaskType = TASK_MAP
			reply.TaskID = allocateID
			reply.FileName = c.files[allocateID]
			c.mapStatus[allocateID] = 1
			// TODO
			// case: worker died
		}
	} else if c.reduceFinished < c.nReduce {
		// reduce job
		allocateID := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceStatus[i] == 0 {
				allocateID = i
				break
			}
		}
		if allocateID == -1 {
			reply.TaskType = TASK_WAITING
		} else {
			reply.TaskType = TASK_REDUCE
			reply.TaskID = allocateID
			c.reduceStatus[allocateID] = 1
			// TODO
			// case: worker died
		}
	} else {
		reply.TaskType = TASK_FINISHED
	}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

	}
	// Your code here.

	c.server()
	return &c
}
