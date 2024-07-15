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
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SortKey []KeyValue

func (a SortKey) Len() int           { return len(a) }
func (a SortKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		response := Heartbeat()
		// log.Printf("Worker: receive coordinator's response, new job is %v \n", response)

		switch response.TaskType {
		case MapTask:
			doMapTask(mapf, response)
			Report(response.TaskID, MapPhase)
		case ReduceTask:
			doReduceTask(reducef, response)
			Report(response.TaskID, ReducePhase)
		case WaittingTask:
			// log.Fatalf("Worker: waitting for tasks")
			time.Sleep(1 * time.Second)
		case DoneTask:
			log.Fatalf("Worker: Done")
			return
		default:
			panic(fmt.Sprintf("worker get an unexpected TaskType %v", response.TaskType))
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, response TaskResponse) {
	filename := response.FileName

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("doMapTask: cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("doMapTask: cannot read %v", filename)
	}
	file.Close()

	intermediate := mapf(filename, string(content))

	nReduce := response.NReduce
	HashKV := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		HashKV[ihash(kv.Key)%nReduce] = append(HashKV[ihash(kv.Key)%nReduce], kv)
	}

	var wfwg sync.WaitGroup
	for reduceId, kvList := range HashKV {
		wfwg.Add(1)
		// 并行写入中间文件
		go func(reduceId int, kvList []KeyValue) {
			defer wfwg.Done()
			fileName := geneInterFileName(response.TaskID, reduceId)
			file, err := os.Create(fileName)
			if err != nil {
				log.Fatalf("doMapTask: cannot create %v", fileName)
			}
			enc := json.NewEncoder(file)
			for _, kv := range kvList {
				err := enc.Encode(kv)
				if err != nil {
					log.Fatalf("doMapTask: cannot encode %v", kv)
				}
			}
			file.Close()
		}(reduceId, kvList)
	}
	wfwg.Wait()
}

func geneInterFileName(mapNumber, reduceNumber int) string {
	return fmt.Sprintf("mr-%d-%d", mapNumber, reduceNumber)
}

func doReduceTask(reducef func(string, []string) string, response TaskResponse) {
	// 首先将来自不同map的多个中间文件合并一起，之后排序进行处理
	kvList := shuffle(response)

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("doReduceTask: Failed to create temp file", err)
	}
	i := 0
	for i < len(kvList) {
		j := i + 1
		for j < len(kvList) && kvList[j].Key == kvList[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvList[k].Value)
		}
		output := reducef(kvList[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kvList[i].Key, output)
		i = j
	}
	tempFile.Close()

	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", response.TaskID)
	os.Rename(tempFile.Name(), fn)
}

func shuffle(response TaskResponse) []KeyValue {
	var kvList []KeyValue
	for mapId := 0; mapId < response.NMap; mapId++ {
		filename := geneInterFileName(mapId, response.TaskID)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("shuffle:cannot open %v", filename)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				log.Fatalf("shuffle: cannot decode %v", filename)
			}
			kvList = append(kvList, kv)
		}
	}
	sort.Sort(SortKey(kvList))
	return kvList
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func Heartbeat() TaskResponse {
	args := TaskRequest{}
	reply := TaskResponse{}

	ok := call("Coordinator.HeartbeatRPCs", &args, &reply)
	if !ok {
		log.Printf("Heartbeat failed")
		reply.TaskType = DoneTask
	}
	return reply
}

func Report(taskId int, phase Phase) {
	args := TaskRequest{TaskID: taskId, Phase: phase}
	reply := TaskResponse{}

	for {
		ok := call("Coordinator.ReportRPCs", &args, &reply)
		if ok {
			break
		}
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
