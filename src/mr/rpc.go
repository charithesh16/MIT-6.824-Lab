package mr

import (
	"os"
	"strconv"

	"github.com/google/uuid"
)

type TaskType int // 0 for Map and 1 for Reduce, -1 for no task available

type TaskRequest struct {
	WorkerId uuid.UUID
}

type TaskResponse struct {
	TaskType   TaskType
	TaskNumber int
	FileName   []string
	NReduce    int
}

type NotifyTaskCompletionRequest struct {
	TaskType         TaskType
	TaskNumber       int
	WrittenFileNames []string
	TaskOutcome      bool
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
