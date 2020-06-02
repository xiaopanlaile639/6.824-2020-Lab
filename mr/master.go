package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	UnfinFileName []string
	MapIds        int

	InterFiles []string
	InterFilesLock  sync.Mutex

	//interMide

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//响应Map任务请求
func (m*Master) ReplyForMapTask(args * CallForMapTaskArgs,reply *CallForMapTaskReplyArgs)error {

	if len(m.UnfinFileName) != 0 {
		reply.FileName = m.UnfinFileName[0]
		reply.TaskId = m.MapIds +1

		m.UnfinFileName = m.UnfinFileName[1:] //去掉已分配的第0个文件
		m.MapIds +=1                          //任务号加1


	} else {
		reply.FileName =""			//返回空字符串
	}

	return nil

}

//响应MapWorker结束的任务
func (m*Master) ReplyForMapFinish(args * CallForMapFinishArgs,reply *CallForMapFinishReplyArgs)error {

	if len(args.FileNames ) != 0{

		m.InterFilesLock.Lock()
		m.InterFiles = append(m.InterFiles,args.FileNames...)		//互斥添加到中间文件中去
		m.InterFilesLock.Unlock()

	}

	reply.Ok = true

	return nil
}

//响应Reduce请求
func (m*Master) ReplyForReduceTask(args* CallForReduceTaskArgs,reply *CallForReduceTaskReplyArgs ) error{

	reduceId:=args.ReduceId			//请求的reduceId

	m.InterFilesLock.Lock()

	for _,interFileName:= range m.InterFiles{

		tmpReduceId,_ := strconv.Atoi(string(interFileName[5]))

		if tmpReduceId == reduceId {
			reply.ReduceFileNames = append(reply.ReduceFileNames,interFileName)
		}
	}
	reply.Ok = true
	m.InterFilesLock.Unlock()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	//Worker()


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.UnfinFileName = files[:]
	m.MapIds = 0
	//m.mapids = make([]int,10,20)




	m.server()
	return &m
}
