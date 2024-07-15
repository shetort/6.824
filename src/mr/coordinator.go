package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	files []string
	tasks []Task

	taskAllDone bool
	phase       Phase

	nMap    int
	nReduce int

	heartBeatCh chan *TaskResponse //需要用这个来在主协程中分配任务
	reportCh    chan *TaskRequest  //需要请求的worker的taskid来改变tasks的状态

	heartBeatTemp *TaskResponse
	reportTemp    *TaskRequest

	doneCh chan bool
	okCh   chan bool
}

const (
	MaxTaskRunInterval = time.Second * 10
)

// 如何同步和保证并发的安全性
// 使用channel

type Task struct {
	taskID     int
	fileName   string
	startTime  time.Time
	taskStatus TaskStatus
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	Waitting
	Done
)

// Your code here -- RPC handlers for the worker to call.

// 分配任务不需要并发，并发需求在任务的执行中
// 一个函数来处理任务的分配，此函数中需要注意，要么用锁，要么用通道来保证安全
// 一个函数需要将所有的任务初始化

func (c *Coordinator) initMapTask() {
	c.phase = MapPhase
	c.tasks = make([]Task, c.nMap)

	for i, file := range c.files {
		c.tasks[i] = Task{
			taskID:     i,
			fileName:   file,
			taskStatus: Idle,
		}
	}
}

func (c *Coordinator) initReduceTask() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)

	for i := range c.tasks {
		c.tasks[i] = Task{
			taskID:     i,
			taskStatus: Idle,
		}
	}
}

func (c *Coordinator) initDoneTask() {
	c.phase = DonePhase
	c.doneCh <- true
}

// coordinator不间断监听来自worker的信号（使用信道来监听，一旦h或者r的某个信道被填入信息，那么就会触发）
// 所以首先需要在heartbeat或者report函数中向信道中注入数据，之后又使用另一个信号ss来阻塞，等待“主”协程处理
// 主协程处理结束后，将数据注入ss中，这样又返回h或者r函数中，结束该函数
// 之后返回值给worker，进行作业处理

func (c *Coordinator) selectNewTask(heartbeat *TaskResponse) bool {

	allTaskDone := true
	selectSuccess := false

	for i, task := range c.tasks {
		switch task.taskStatus {
		case Idle:
			allTaskDone = false
			selectSuccess = true
			c.tasks[i].taskStatus = Waitting
			c.tasks[i].startTime = time.Now()

			if c.phase == MapPhase {
				heartbeat.TaskType = MapTask
			}
			if c.phase == ReducePhase {
				heartbeat.TaskType = ReduceTask
			}
			heartbeat.TaskID = i
			heartbeat.FileName = c.tasks[i].fileName
			heartbeat.NReduce = c.nReduce
			heartbeat.NMap = c.nMap
		case Waitting:
			allTaskDone = false
			if time.Since(task.startTime) > MaxTaskRunInterval {
				selectSuccess = true
				c.tasks[i].taskStatus = Waitting
				c.tasks[i].startTime = time.Now()

				if c.phase == MapPhase {
					heartbeat.TaskType = MapTask
				}
				if c.phase == ReducePhase {
					heartbeat.TaskType = ReduceTask
				}
				heartbeat.TaskID = i
				heartbeat.FileName = c.tasks[i].fileName
				heartbeat.NReduce = c.nReduce
				heartbeat.NMap = c.nMap
			}
		case Done:
			continue
		default:
			log.Printf("Wrong TaskType")
			continue
		}

		if selectSuccess {
			break
		}
	}

	if !allTaskDone && !selectSuccess {
		heartbeat.TaskType = WaittingTask
	}

	if allTaskDone {
		heartbeat.TaskType = DoneTask
	}

	return allTaskDone
}

func (c *Coordinator) allot() {
	for {
		select {
		case heartbeat := <-c.heartBeatCh:
			isAllDone := c.selectNewTask(heartbeat)
			if isAllDone {
				c.ChangePhase()
				c.selectTaskAfterSwitchPhase(heartbeat)
			}
			c.okCh <- true
		case report := <-c.reportCh:
			// log.Printf("allot: report recive- %v\n", report)
			if report.Phase == c.phase {
				c.tasks[report.TaskID].taskStatus = Done
			}
			c.okCh <- true
		}
	}
}

func (c *Coordinator) selectTaskAfterSwitchPhase(reply *TaskResponse) {
	switch c.phase {
	case ReducePhase:
		isAllTaskDone := c.selectNewTask(reply)
		if isAllTaskDone {
			c.ChangePhase()
			c.selectTaskAfterSwitchPhase(reply)
		}
	case DonePhase:
		reply.TaskType = DoneTask
	}
}

// 转换阶段函数
func (c *Coordinator) ChangePhase() {
	switch c.phase {
	case MapPhase:
		// log.Printf("Coordinator: %v is done, start %v\n", c.phase, ReducePhase)
		c.phase = ReducePhase
		c.initReduceTask()
	case ReducePhase:
		// log.Printf("Coordinator: %v is done, all tasks are finished\n", c.phase)
		c.phase = DonePhase
		c.initDoneTask()
	case DonePhase:
		log.Printf("Coordinator: Get an unexpected heartbeat in %v\n", c.phase)
	default:
		log.Printf("Phase Wrong")
	}
}

func (c *Coordinator) HeartbeatRPCs(args *TaskRequest, reply *TaskResponse) error {
	c.heartBeatCh <- reply
	<-c.okCh
	return nil
}

func (c *Coordinator) ReportRPCs(args *TaskRequest, reply *TaskResponse) error {
	c.reportCh <- args
	<-c.okCh
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
	<-c.doneCh
	log.Printf("Coordinator: Done\n")
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:         files,
		taskAllDone:   false,
		phase:         MapPhase,
		nMap:          len(files),
		nReduce:       nReduce,
		heartBeatCh:   make(chan *TaskResponse),
		reportCh:      make(chan *TaskRequest),
		heartBeatTemp: new(TaskResponse),
		reportTemp:    new(TaskRequest),
		doneCh:        make(chan bool, 1),
		okCh:          make(chan bool),
	}

	c.initMapTask()
	go c.allot()

	go c.server()
	return &c
}
