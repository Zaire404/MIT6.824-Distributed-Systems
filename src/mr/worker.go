package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// periodically ask coordinator for task
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AllocateTask", &args, &reply)
		if !ok {
			// retry calling for task
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}
		if reply.TaskType == FINISHED_TASK {
			break
		}
		switch reply.TaskType {
		case MAP_TASK:
			// input
			file, err := os.Open(reply.FileName)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}

			// map function
			intermediate := mapf(reply.FileName, string(content))

			// partitioning with hash function
			buckets := make([][]KeyValue, reply.NReduce)
			for _, kv := range intermediate {
				index := ihash(kv.Key) % reply.NReduce
				buckets[index] = append(buckets[index], kv)
			}

			// output
			for i := range buckets {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				// oname := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)
				ofile, err := ioutil.TempFile("", "mr-tmp*")
				defer ofile.Close()
				if err != nil {
					log.Fatalf("cannot create temp file")
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range buckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write into %v", oname)
					}
				}
				err = MoveFile(ofile.Name(), oname)
				if err != nil {
					fmt.Println(err)
					log.Fatalf("cannot rename %v to %v", ofile.Name(), oname)
				}
			}

			// call master to send the finish message
			finishedArgs := WorkerArgs{reply.TaskID}
			finishedReply := WorkerReply{}
			call("Coordinator.ReceiveFinishedMap", &finishedArgs, &finishedReply)
		case REDUCE_TASK:
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)
				file, err := os.Open(iname)
				defer file.Close()
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			// output
			oname := "mr-out-" + strconv.Itoa(reply.TaskID)
			ofile, _ := ioutil.TempFile("", "mr-tmp*")
			defer ofile.Close()
			intermediate_len := len(intermediate)
			for i := 0; i < intermediate_len; i++ {
				values := []string{}
				j := i + 1
				for ; j < intermediate_len; j++ {
					if intermediate[i].Key != intermediate[j].Key {
						break
					}
				}
				for ; i < j; i++ {
					values = append(values, intermediate[i].Value)
				}
				i--
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			}

			err := MoveFile(ofile.Name(), oname)
			if err != nil {
				fmt.Println(err)
				log.Fatalf("cannot rename %v to %v", ofile.Name(), oname)
			}

			// remove intermediate files
			for i := 0; i < reply.NMap; i++ {
				// iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)
				name := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
				err := os.Remove(name)
				if err != nil {
					log.Fatalf("cannot delete" + name)
				}
			}

			finishedArgs := WorkerArgs{reply.TaskID}
			finishedReply := WorkerReply{}
			call("Coordinator.ReceiveFinishedReduce", &finishedArgs, &finishedReply)
		}
	}
}

func MoveFile(sourcePath, destPath string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}
	outputFile, err := os.Create(destPath)
	if err != nil {
		inputFile.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, inputFile)
	inputFile.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}
	// The copy was successful, so now delete the original file
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
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
