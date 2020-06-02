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
//请求Map任务args
type CallForMapTaskArgs struct {

	Test int

}
//请求Map任务reply
type  CallForMapTaskReplyArgs struct {
	FileName string //返回的文件名
	TaskId   int    //返回的任务编号
}

//请求Map完成args
type CallForMapFinishArgs struct{
	FileNames []string
	TaskId int
}
//请求Map完成reply
type CallForMapFinishReplyArgs struct{
	Ok bool
}

//请求reduce任务args
type CallForReduceTaskArgs struct {
	ReduceId int
}

//请求reduce任务reply
type  CallForReduceTaskReplyArgs struct {
	ReduceFileNames []string
	Ok              bool
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
