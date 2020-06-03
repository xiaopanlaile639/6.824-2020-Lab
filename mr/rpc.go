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
	TestPid int
}

//请求Map任务reply
type CallForMapTaskReplyArgs struct {
	FileName string //返回的文件名
	MapId    int    //返回的任务编号
}

//请求Map完成args
type CallForMapFinishArgs struct {
	FileNames []string
	MapId     int
}

//请求Map完成reply
type CallForMapFinishReplyArgs struct {
	Ok bool
}

//请求是否全部Map任务都已运行结束
type CallForAllMapFinishArgs struct {
	MapId int
}

type CallForAllMapFinishReplyArgs struct {
	IsFinished bool
	Ok         bool
}

//请求reduce任务args
type CallForReduceTaskArgs struct {
	ReduceId int
	//TestPid int
}

//请求reduce任务reply
type CallForReduceTaskReplyArgs struct {
	ReduceId        int
	ReduceFileNames []string
	Ok              bool
}

//请求返回 完成的Reduce任务编号
type CallForReduceFinishArgs struct {
	ReduceId int
}

type CallForReduceFinishReplyArgs struct {
	OK bool
}

//请求是否所有的Reduce任务都已经完成
type CallForAllReduceFinishArgs struct {
	MapId int
}

type CallForAllReduceFinishReplyArgs struct {
	IsFinished bool
	Ok         bool
}

//同时Master所有任务均已完成
type CallForAllTaskFinishArgs struct {
	Test int
}

type CallForAllTaskFinishReplyArgs struct {
	Test int
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
