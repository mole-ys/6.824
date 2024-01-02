package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// go run mrcoordinator.go wc.so pg*txt

type Coordinator struct {
	// Your definitions here.
	// worker id
	IdMap map[int]int
	// 文件名
	FileNames []string
	// 任务 key为文件名，value为是否完成
	Tasks map[string]bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Task(req *TaskReq, res *TaskRes) error {
	_, exists := c.IdMap[req.Id]
	if (!exists) {
		fmt.Println("有新worker加入, id为", req.Id)
		c.IdMap[req.Id] = 1
	}else {
		fmt.Printf("worker %d 开始取任务\n", req.Id)
	}

	res.Task = "Map"
	for filename, done := range c.Tasks {
		if(!done){
			res.FileName = filename
			fmt.Println(filename)
		}
	}
	
	
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.IdMap = make(map[int]int)
	c.Tasks = make(map[string]bool)
	for _, filename := range os.Args[2:] {
		c.FileNames = append(c.FileNames, filename)
		// fmt.Println(filename)
		c.Tasks[filename] = false
	}

	// fmt.Println(c.FileNames[0])
	c.server()
	return &c
}
