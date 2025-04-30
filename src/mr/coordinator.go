package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mapTasks map[int]Task    // keep track of map tasks
var reduceTasks map[int]Task // keep track of reduce tasks
var taskMutex sync.Mutex

type Task struct {
	taskNumber   int
	fileName     []string
	status       int // 0(not started),1(inprogress),2(completed)
	taskType     TaskType
	assignedTime time.Time
}

type Coordinator struct {
	nReduce int
}

func (c *Coordinator) GetWork(request *TaskRequest, response *TaskResponse) error {
	availableTask, err := getAvailableTask()
	if err != nil {
		response.TaskType = -1
		return nil
	}
	response.TaskType = availableTask.taskType
	response.TaskNumber = availableTask.taskNumber
	response.FileName = availableTask.fileName
	response.NReduce = c.nReduce
	// go checkForTaskCompletion(availableTask.taskNumber, availableTask.taskType)
	return nil
}

// func checkForTaskCompletion(taskNumber int, taskType TaskType) {
// 	time.Sleep(10 * time.Second)

// 	taskMutex.Lock()
// 	defer taskMutex.Unlock()

// 	if taskType == 0 {
// 		if task, ok := mapTasks[taskNumber]; ok && task.status != 2 {
// 			task.status = 0
// 			mapTasks[taskNumber] = task
// 		}
// 	} else if taskType == 1 {
// 		if task, ok := reduceTasks[taskNumber]; ok && task.status != 2 {
// 			task.status = 0
// 			reduceTasks[taskNumber] = task
// 		}
// 	}
// }

// func getAvailableTask() (Task, error) {
// 	taskMutex.Lock()
// 	defer taskMutex.Unlock()
// 	for k, _ := range mapTasks {
// 		if mapTasks[k].status == 0 {
// 			task := mapTasks[k]
// 			task.status = 1
// 			mapTasks[k] = task
// 			return mapTasks[k], nil
// 		}
// 	}
// 	for k, _ := range reduceTasks {
// 		if reduceTasks[k].status == 0 {
// 			task := reduceTasks[k]
// 			task.status = 1
// 			reduceTasks[k] = task
// 			return reduceTasks[k], nil
// 		}
// 	}
// 	return Task{}, fmt.Errorf("no available tasks")
// }

func getAvailableTask() (Task, error) {
	timeoutDuration := 10 * time.Second // Timeout before considering a task failed
	taskMutex.Lock()
	defer taskMutex.Unlock()
	// --- Check for Map Tasks ---

	// Preference 1: Find a map task that hasn't started (status 0)
	for i := 0; i < len(mapTasks); i++ { // Iterate safely if map size known or use range
		task := mapTasks[i] // Assuming mapTasks uses 0-based integer keys matching taskNumber
		if task.status == 0 {
			task.status = 1 // Mark as in progress
			task.assignedTime = time.Now()
			mapTasks[i] = task // Update the map
			// fmt.Printf("Assigning new Map task %d\n", task.taskNumber)
			return task, nil
		}
	}

	// Preference 2: Find a map task that seems timed out (status 1)
	for i := 0; i < len(mapTasks); i++ {
		task := mapTasks[i]
		// Check if in progress AND enough time has passed since assignment
		if task.status == 1 && time.Since(task.assignedTime) > timeoutDuration {
			task.assignedTime = time.Now() // Reset assignment time for the new worker
			mapTasks[i] = task             // Update the map (status remains 1)
			// fmt.Printf("Re-assigning timed-out Map task %d\n", task.taskNumber)
			return task, nil
		}
	}

	// --- Check if Map Phase is Complete ---
	allMapTasksDone := true
	for i := 0; i < len(mapTasks); i++ {
		if mapTasks[i].status != 2 {
			allMapTasksDone = false
			break
		}
	}

	// If map phase not done, no reduce tasks can be assigned yet
	if !allMapTasksDone {
		// fmt.Println("Waiting for map tasks to complete...")
		return Task{}, nil // No assignable tasks right now
	}

	// --- Check for Reduce Tasks ---

	// Preference 1: Find a reduce task that hasn't started (status 0)
	for i := 0; i < len(reduceTasks); i++ { // Assuming reduceTasks uses 0-based integer keys
		task, exists := reduceTasks[i]
		if !exists { // Should not happen if initialized correctly
			log.Printf("Warning: Reduce task %d not found during assignment check.", i)
			continue
		}
		if task.status == 0 {
			task.status = 1 // Mark as in progress
			task.assignedTime = time.Now()
			reduceTasks[i] = task // Update the map
			// fmt.Printf("Assigning new Reduce task %d\n", task.taskNumber)
			return task, nil
		}
	}

	// Preference 2: Find a reduce task that seems timed out (status 1)
	for i := 0; i < len(reduceTasks); i++ {
		task, exists := reduceTasks[i]
		if !exists {
			continue
		}
		// Check if in progress AND enough time has passed since assignment
		if task.status == 1 && time.Since(task.assignedTime) > timeoutDuration {
			task.assignedTime = time.Now() // Reset assignment time for the new worker
			reduceTasks[i] = task          // Update the map (status remains 1)
			// fmt.Printf("Re-assigning timed-out Reduce task %d\n", task.taskNumber)
			return task, nil
		}
	}

	// No tasks left to assign
	// fmt.Println("No available tasks found.")
	return Task{}, nil
}

func (c *Coordinator) NotifyTaskCompletion(request *NotifyTaskCompletionRequest, response *struct{}) error {

	taskMutex.Lock()
	defer taskMutex.Unlock()

	if request.TaskType == 0 {
		if task, ok := mapTasks[request.TaskNumber]; ok && task.status == 1 {
			task.status = 2
			mapTasks[request.TaskNumber] = task
			for _, fileName := range request.WrittenFileNames {
				reduceTaskNumber := getReduceTaskNumber(fileName)
				if reduceTask, ok := reduceTasks[reduceTaskNumber]; ok {
					reduceTask.fileName = append(reduceTask.fileName, fileName)
					reduceTasks[reduceTaskNumber] = reduceTask
				} else {
					reduceTasks[reduceTaskNumber] = Task{
						taskNumber:   reduceTaskNumber,
						fileName:     []string{fileName},
						status:       0,
						taskType:     1,
						assignedTime: time.Now(),
					}
				}
			}
		}
	} else if request.TaskType == 1 {
		if task, ok := reduceTasks[request.TaskNumber]; ok && task.status == 1 {
			task.status = 2
			reduceTasks[request.TaskNumber] = task
		}
	}
	return nil
}

func getReduceTaskNumber(fileName string) int {
	fileName = strings.TrimSuffix(fileName, ".txt")
	parts := strings.Split(fileName, "-")
	if len(parts) != 3 {
		log.Fatalf("Invalid file name format: %s", fileName)
	}
	reduceTaskNumber, err := strconv.Atoi(parts[2])
	if err != nil {
		log.Fatalf("Failed to parse reduce task number from file name: %s", fileName)
	}

	return reduceTaskNumber
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := false
	taskMutex.Lock()
	for _, task := range mapTasks {
		if task.status != 2 {
			ret = false
			break
		}
		ret = true
	}
	for _, task := range reduceTasks {
		if task.status != 2 {
			ret = false
			break
		}
		ret = true
	}
	taskMutex.Unlock()
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	mapTasks = make(map[int]Task)
	reduceTasks = make(map[int]Task)
	for taskNumber, file := range files {
		mapTasks[taskNumber] = Task{
			taskNumber:   taskNumber,
			fileName:     []string{file},
			status:       0,
			taskType:     0,
			assignedTime: time.Now(),
		}
	}
	c.server()
	return &c
}
