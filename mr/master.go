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



//任务分配状态
const (
	NOTALLOCATED int = -1
	RUNNING      int = 0
	FINISHED     int = 1
)

type MapTask struct {
	TaskFileName string
	status       int
	UsedTime	int
}

type ReduceTask struct {
	ReduceTaskFiles	[]string
	status			int
	UsedTime		int
}



//master结点所要维护的信息
type Master struct {

	AllMapTask []MapTask //所有的Map任务

	AllReduceTask	[]ReduceTask	//所有的reduce任务


	AllMapTasksFinished bool		//Map任务是否执行结束
	ALlReduceTasksFinished bool		//Reduce任务是否执行结束

	AllTaskFinished bool //所有任务是否全部结束

	MasterLock sync.Mutex //互斥锁		//操作元数据需要加锁

}

////////////////////////////////master的rpc服务函数/////////////////////////////////////////////

// Your code here -- RPC handlers for the worker to call.
//响应Map任务请求
func (m *Master) ReplyForMapTask(args *CallForMapTaskArgs, reply *CallForMapTaskReplyArgs) error {

	reply.FileName = "" //默认返回是空的
	reply.MapId = -1
	reply.Ok = false

	m.MasterLock.Lock()

	//遍历寻找未分配的任务
	for mapId, mapTask := range m.AllMapTask {

		//fmt.Printf("asker:pid:%v--mapId:%v--mapTask:%v\n", args.TestPid, mapId, mapTask)

		if mapTask.status == NOTALLOCATED { //未分配


			reply.MapId = mapId
			reply.FileName = mapTask.TaskFileName
			reply.Ok = true

			m.AllMapTask[mapId].status = RUNNING //已分配，正在运行中
			m.AllMapTask[mapId].UsedTime = 0		//初始化任务的时间


			break
		}
	}

	m.MasterLock.Unlock()

	return nil

}

//响应MapWorker结束的任务
func (m *Master) ReplyForMapFinish(args *CallForMapFinishArgs, reply *CallForMapFinishReplyArgs) error {


	m.MasterLock.Lock()

	MapId := args.MapId //完成任务的MapID号

	m.AllMapTask[MapId].status = FINISHED //已完成

	for _,fileName := range args.FileNames{

		//提取Id号
		idInFileNameIndex := len(fileName) - 1 //文件名中的ReduceId号
		tmpReduceId, _ := strconv.Atoi(string(fileName[idInFileNameIndex]))

		//准备Reduce任务的输入文件
		m.AllReduceTask[tmpReduceId].ReduceTaskFiles = append(m.AllReduceTask[tmpReduceId].ReduceTaskFiles,fileName)

	}


	m.MasterLock.Unlock()

	reply.Ok = true
	return nil
}

//请求是否Map任务都结束了
func (m *Master) ReplyForAllMapFinish(args *CallForAllMapFinishArgs, reply *CallForAllMapFinishReplyArgs) error {

	reply.IsFinished = true
	reply.Ok = true

	m.MasterLock.Lock()

	finishedFlag := true
	for _, mapTask := range m.AllMapTask {

		if mapTask.status != FINISHED {
			reply.IsFinished = false //有任务未结束，需要等待

			//m.AllMapTask[i].status = NOTALLOCATED //需要重新分配
			finishedFlag = false
			break
		}
	}

	m.AllMapTasksFinished = finishedFlag		//更新所有Map任务完成标记

	m.MasterLock.Unlock()



	return nil

}

//响应Reduce申请任务请求
func (m *Master) ReplyForReduceTask(args *CallForReduceTaskArgs, reply *CallForReduceTaskReplyArgs) error {

	reply.Ok = false //默认分配失败
	reply.ReduceId = -1

	m.MasterLock.Lock()

	//分配Reduce编号
	for reduceId, tmpReduceTask := range m.AllReduceTask {

		if tmpReduceTask.status == NOTALLOCATED { //未分配

			m.AllReduceTask[reduceId].status = RUNNING //已分配，运行中
			m.AllReduceTask[reduceId].UsedTime = 0		//初始化使用时间

			//返回的Id和文件名
			reply.ReduceFileNames = m.AllReduceTask[reduceId].ReduceTaskFiles
			reply.ReduceId = reduceId
			reply.Ok = true
			break
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

	m.AllReduceTask[reduceId].status = FINISHED //此编号为ReduceId的任务完成

	m.MasterLock.Unlock()

	return nil

}

//回复是否所有的Reduce任务都已经完成
func (m *Master) ReplyForAllReduceFinish(args *CallForAllReduceFinishArgs, reply *CallForAllReduceFinishReplyArgs) error {

	reply.IsFinished = true
	reply.Ok = true

	m.MasterLock.Lock()
	finishedFlag := true
	for _, tmpReduceTask := range m.AllReduceTask {

		if tmpReduceTask.status != FINISHED {

			finishedFlag = false		//未完成标记
			reply.IsFinished = false

			break
		}
	}

	m.ALlReduceTasksFinished = finishedFlag		//更新所有的reduce任务完成标记

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

//给运行中的任务计时
func (m *Master) TimeTick()  {

	m.MasterLock.Lock()
	defer m.MasterLock.Unlock()

	if m.AllMapTasksFinished == false{

		//计时Map任务
		for i,tmpMapTask := range m.AllMapTask {
			if tmpMapTask.status == RUNNING {
				m.AllMapTask[i].UsedTime++		//所用时间更新

				if m.AllMapTask[i].UsedTime > 10 {		//如果运行超过10s
					m.AllMapTask[i].status = NOTALLOCATED		//更新状态为未分配
				}
			}

		}
	} else {
		//计时Reduce任务
		for i,tmpReduceTask := range m.AllReduceTask{
			if tmpReduceTask.status == RUNNING {
				m.AllReduceTask[i].UsedTime++

				if m.AllReduceTask[i].UsedTime > 10 {

					m.AllReduceTask[i].status = NOTALLOCATED
				}
			}
		}
	}
}


//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	m.TimeTick()		//计时
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
		tmpMapTasks := MapTask{fileName, NOTALLOCATED,0} //-1代表任务未分配

		m.AllMapTask = append(m.AllMapTask, tmpMapTasks)
	}

	//m.AllReduceTask = make([]int, 10)

	//初始化Reduce任务
	for i := 0; i < nReduce; i++ {

		tmpReduceTask := ReduceTask{status: NOTALLOCATED,UsedTime: 0}

		m.AllReduceTask = append(m.AllReduceTask,tmpReduceTask)

		//m.AllReduceTask[i] = NOTALLOCATED //Reduce任务未分配
		//m.AllReduceTask[i].status = NOTALLOCATED
		//m.AllReduceTask[i].UsedTime = 0
	}

	m.AllMapTasksFinished = false
	m.ALlReduceTasksFinished = false
	m.AllTaskFinished = false //初始化任务未完成

	m.MasterLock.Unlock()

	m.server()
	return &m
}
