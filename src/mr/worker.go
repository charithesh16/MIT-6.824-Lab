package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := uuid.New()
	for {
		work := getWork(workerId)
		filename := work.FileName
		taskNumber := work.TaskNumber
		nReduce := work.NReduce
		if work.TaskType == 0 {
			for _, file := range filename {
				performMapTask(taskNumber, nReduce, file, mapf)
			}
		} else if work.TaskType == 1 {
			performReduceTask(taskNumber, nReduce, filename, reducef)
		} else {
			fmt.Println("No available task")
		}
		time.Sleep(10 * time.Second)
	}
}

func performReduceTask(taskNumber int, nReduce int, filenames []string, reducef func(string, []string) string) {

	intermediate := []KeyValue{}
	for m := 0; m < len(filenames); m++ {
		file, err := os.Open(filenames[m])
		if err != nil {
			log.Fatalf("cannot open %v", filenames[m])
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	keyValueCount := make(map[string][]string)
	for _, kv := range intermediate {
		keyValueCount[kv.Key] = append(keyValueCount[kv.Key], kv.Value)
	}
	oname := fmt.Sprintf("mr-out-%d", taskNumber)
	ofile, _ := ioutil.TempFile("", oname)

	for k, v := range keyValueCount {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
	ofile.Close()

	// Rename output file
	os.Rename(ofile.Name(), oname)
	filesAsList := []string{oname}
	notifyCoordinator(taskNumber, filesAsList, 1)
}

func performMapTask(taskNumber int, nReduce int, filename string, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))

	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		intermediate[r] = append(intermediate[r], kv)
	}
	filesAsList := make([]string, 0, nReduce)
	for r, kva := range intermediate {
		oname := fmt.Sprintf("mr-%d-%d", taskNumber, r)
		ofile, _ := ioutil.TempFile("", oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
		filesAsList = append(filesAsList, oname)

	}
	notifyCoordinator(taskNumber, filesAsList, 0)
}

func notifyCoordinator(taskNumber int, intermediateFiles []string, taskType TaskType) {
	notifyRest := NotifyTaskCompletionRequest{
		TaskNumber:       taskNumber,
		TaskType:         taskType,
		WrittenFileNames: intermediateFiles,
	}
	call("Coordinator.NotifyTaskCompletion", &notifyRest, nil)

}

func writeToFile(taskNumber int, reduceTaskNumber int, key string, value string, taskType TaskType) string {
	// Determine the directory and file name
	var intermediateFileName string
	if taskType == 0 {
		intermediateFileName = fmt.Sprintf("intermediate/mr-%d-%d.txt", taskNumber, reduceTaskNumber)
		// Ensure the "intermediate" directory exists
		err := os.MkdirAll("intermediate", os.ModePerm)
		if err != nil {
			log.Fatalf("cannot create intermediate directory: %v", err)
		}
	} else if taskType == 1 {
		intermediateFileName = fmt.Sprintf("final/mr-out-%d.txt", reduceTaskNumber)
		// Ensure the "final" directory exists
		err := os.MkdirAll("final", os.ModePerm)
		if err != nil {
			log.Fatalf("cannot create final directory: %v", err)
		}
	}

	// Open the file in append mode, create it if it doesn't exist
	file, err := os.OpenFile(intermediateFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("cannot open %v", intermediateFileName)
	}
	defer file.Close()

	// Write the key-value pair to the file
	if _, err := file.WriteString(key + " " + value + "\n"); err != nil {
		log.Fatalf("cannot write to %v", intermediateFileName)
	}

	return intermediateFileName
}

func getWork(workerId uuid.UUID) TaskResponse {
	request := TaskRequest{WorkerId: workerId}
	response := TaskResponse{}
	ok := call("Coordinator.GetWork", &request, &response)
	if ok {
		return response
	}
	return response
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
