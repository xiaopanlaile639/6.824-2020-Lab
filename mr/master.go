package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//type  MapTask struct {
//	MapId int
//	FileName string
//}

const (
	NOTALLOCATED int = -1
	RUNNING      int = 0
	FINISHED     int = 1
)

type MapTasks struct {
	TaskFileName string
	status       int
}

type Master struct {
	// Your definitions here.
	AllMapTask []MapTasks //所有的Map任务

	AllReduceTask []int //Reduce任务
	InterFiles    []string

	//NextMapId	int
	//MapIds        int

	AllTaskFinished bool //所有任务是否全部结束

	MasterLock sync.Mutex //互斥锁

}

// Your code here -- RPC handlers for the worker to call.
//响应Map任务请求
func (m *Master) ReplyForMapTask(args *CallForMapTaskArgs, reply *CallForMapTaskReplyArgs) error {

	reply.FileName = "" //默认返回是空的
	reply.MapId = -1

	m.MasterLock.Lock()

	//遍历寻找未分配的任务
	for mapId, mapTask := range m.AllMapTask {

		fmt.Printf("pid:%v--mapId:%v--mapTask:%v--status:%v\n", args.TestPid, mapId, mapTask, NOTALLOCATED)

		if mapTask.status == NOTALLOCATED { //未分配

			fmt.Printf("allocate mapid:%v to pid:%v\n", mapId, args.TestPid)

			reply.MapId = mapId
			reply.FileName = mapTask.TaskFileName

			m.AllMapTask[mapId].status = RUNNING //已分配，正在运行中

			break
		}
	}

	m.MasterLock.Unlock()

	return nil

}

//响应MapWorker结束的任务
func (m *Master) ReplyForMapFinish(args *CallForMapFinishArgs, reply *CallForMapFinishReplyArgs) error {

	//if len(args.FileNames ) != 0{

	m.MasterLock.Lock()

	MapId := args.MapId //完成任务的MapID号

	//m.CurReduceTask[MapId] = true
	m.AllMapTask[MapId].status = FINISHED //已完成

	m.InterFiles = append(m.InterFiles, args.FileNames...) //互斥添加到中间文件中去
	m.MasterLock.Unlock()

	//}

	reply.Ok = true
	return nil
}

//请求是否Map任务都结束了
func (m *Master) ReplyForAllMapFinish(args *CallForAllMapFinishArgs, reply *CallForAllMapFinishReplyArgs) error {

	reply.IsFinished = true

	m.MasterLock.Lock()

	for i, mapTask := range m.AllMapTask {

		if mapTask.status != FINISHED {
			reply.IsFinished = false //有任务未结束，需要重新分配

			m.AllMapTask[i].status = NOTALLOCATED //需要重新分配
			break
		}
	}

	m.MasterLock.Unlock()

	reply.Ok = true

	return nil

}

//响应Reduce请求
func (m *Master) ReplyForReduceTask(args *CallForReduceTaskArgs, reply *CallForReduceTaskReplyArgs) error {

	reply.Ok = false //默认分配失败
	reply.ReduceId = -1

	m.MasterLock.Lock()

	//分配Reduce编号
	for reduceId, status := range m.AllReduceTask {
		if status == NOTALLOCATED { //未分配

			m.AllReduceTask[reduceId] = RUNNING //已分配，运行中
			reply.Ok = true
			reply.ReduceId = reduceId
			break
		}
	}

	if reply.ReduceId != -1 { //已分配ReduceId
		//分配具体的文件
		for _, interFileName := range m.InterFiles {

			idInFileNameIndex := len(interFileName) - 1 //文件名中的ReduceId号
			tmpReduceId, _ := strconv.Atoi(string(interFileName[idInFileNameIndex]))

			if tmpReduceId == reply.ReduceId { //文件名中的ReduceId号和已分配的ID号相同
				reply.ReduceFileNames = append(reply.ReduceFileNames, interFileName)
			}

		}
	}

	m.MasterLock.Unlock()

	return nil
}

//标记reduceId的任务完成
func (m *Master) ReplyForReduceFinish(args *CallForReduceFinishArgs, reply *CallForReduceFinishReplyArgs) error {

	reply.OK = true
	reduceId := args.ReduceId

	m.MasterLock.Lock()

	m.AllReduceTask[reduceId] = FINISHED //此编号为ReduceId的任务完成

	m.MasterLock.Unlock()

	return nil

}

//回复是否所有的Reduce任务都已经完成
func (m *Master) ReplyForAllReduceFinish(args *CallForAllReduceFinishArgs, reply *CallForAllReduceFinishReplyArgs) error {

	reply.IsFinished = true
	reply.Ok = true

	m.MasterLock.Lock()
	for i, reduceTaskStatus := range m.AllReduceTask {

		if reduceTaskStatus != FINISHED {
			reply.IsFinished = false
			m.AllReduceTask[i] = NOTALLOCATED //等待重新分配
			break
		}
	}

	m.MasterLock.Unlock()

	return nil
}

//不使用参数，无返回值，只是标记所有任务均已完成
func (m *Master) ReplyForAllTaskFinish(args *CallForAllTaskFinishArgs, reply *CallForAllTaskFinishReplyArgs) error {

	m.MasterLock.Lock()
	m.AllTaskFinished = true //标记所有任务均已完成
	m.MasterLock.Unlock()

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

	m.MasterLock.Lock()

	ret = m.AllTaskFinished

	m.MasterLock.Unlock()

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

	m.MasterLock.Lock()

	//初始化传递进来的所有input文件名，map task的输入
	for _, fileName := range files {
		tmpMapTasks := MapTasks{fileName, NOTALLOCATED} //-1代表任务未分配

		m.AllMapTask = append(m.AllMapTask, tmpMapTasks)
	}

	m.AllReduceTask = make([]int, 10)

	//初始化Reduce任务
	for i := 0; i < nReduce; i++ {
		m.AllReduceTask[i] = NOTALLOCATED //Reduce任务未分配
	}

	m.AllTaskFinished = false //初始化任务未完成

	m.MasterLock.Unlock()

	m.server()
	return &m
}
