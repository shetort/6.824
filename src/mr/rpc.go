package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type Phase int
type TaskType int

type TaskRequest struct {
	TaskID int
	Phase  Phase
}
type TaskResponse struct {
	TaskType TaskType
	TaskID   int
	FileName string
	NReduce  int
	NMap     int
}

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	DoneTask
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
