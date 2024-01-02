package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// import "os"
// import "io/ioutil"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.


	// 使用时间作为ID
	currentTime := time.Now().Unix()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for(true){
		task, filename, id := CallForTask(int(currentTime))
		if(task == "Map"){
			succeed, err := MapTask(mapf, filename, id)
			if err != nil {
				fmt.Printf("task failed: %v %v %v", task, filename, id)
			} else{
				// 可能需要加重试
				SendResult(filename, succeed, task)
			}

		}
		time.Sleep(10 * time.Second)
	}
}

func MapTask(mapf func(string, string) []KeyValue, filename string, id int) (bool, error) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("cannot open %v", filename)
		return false, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read %v", filename)
		return false, err
	}
	file.Close()
	// 调用wc中的map函数
	// 最后返回一个切片
	kva := mapf(filename, string(content))
	mrname := fmt.Sprintf("mr-%d-%d.txt", id, id)
	CombineAndSave(kva, mrname)
	// fmt.Fprintf(f, "%v \n", kva)

	return true, nil
}

func CombineAndSave(kva []KeyValue, filename string)  {
	sort.Sort(ByKey(kva))

	savefile, err := os.Create("intermediatesave/" + filename)
	if err != nil {
		log.Fatalf(err.Error())
		log.Fatalf("cannot create %v", filename)
	}
	defer savefile.Close()
	enc := json.NewEncoder(savefile)
	for kv := range kva {
		// fmt.Println(kva[kv])
    	err := enc.Encode(&kva[kv])
		if err != nil {
			log.Fatalf("Save failed :" + err.Error())
		}
	}
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallForTask(currentTime int) (string, string, int) {

	// declare an argument structure.
	req := TaskReq{}

	// fill in the argument(s).
	req.Id = currentTime

	// declare a reply structure.
	reply := TaskRes{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Task", &req, &reply)
	if ok {
		fmt.Printf("任务类型 %s\n", reply.Task)
		fmt.Printf("处理文件 %s\n", reply.FileName)	
	} else {
		fmt.Printf("Task call failed!\n")
	}
	return reply.Task, reply.FileName, reply.TaskId
}

func SendResult(filename string, succeed bool, task string) (bool) {
	// declare an argument structure.
	req := ResultReq{}

	// fill in the argument(s).
	req.FileName = filename
	req.Succeed = succeed
	req.Task = task

	// declare a reply structure.
	reply := ResultRes{}
	ok := call("Coordinator.Result", &req, &reply)
	if !ok {
		fmt.Printf("sendresult call failed!\n")
	}
	return reply.Received
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
