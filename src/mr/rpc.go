package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 获取任务
type TaskReq struct {
	Id int //给worker一个id
}
type TaskRes struct {
	Task string //任务类型
	FileName string //文件名
	TaskId int // 任务号
}

// 任务响应
type ResultReq struct {
	Task string //任务类型
	FileName string //文件名
	Succeed bool //是否成功
}
type ResultRes struct {
	Received bool //是否接受到
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
