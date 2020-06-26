package mr

import (
	"encoding/json"
	"sort"
	"time"

	//"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	//	"strconv"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//中间数据
type IntermidData struct {
	Intermediate []KeyValue
//	InterLock    sync.Mutex //保护中间数据的锁
}


type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var mapWriteLocks [10]sync.Mutex //保护中间数据的锁
var writeLock sync.Mutex
var interData IntermidData //全局变量，Map得到的中间数据

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//map任务:或许应该把所有的中间数据暂时放在内存中，完全结束之后再放在文件系统中。
func MapWorker(mapf func(string, string) []KeyValue, fileName string, MapId int) error {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kvRet := mapf(fileName, string(content)) //执行用户提供的Map函数，返回键值对

	////将kv写入文件，并通知master///
	ok, outFileNames := WriteToIntFile(kvRet, MapId) //写入到中间文件中去

	if !ok {
		fmt.Printf("mapTask %v failed.\n", MapId)
	} else {
		fmt.Printf("mapTask：%v finished.\n", MapId)
		CallForMapFinishReply(outFileNames, MapId) //返回给master写入的文件名信息
	}
	return nil

}

//写入到中间文件中去
func WriteToIntFile(mapInterData []KeyValue, mapId int) (bool, []string) {

	reduceNum := 10 //默认reducer的数量为10

	//初始化一个二维切片
	tmpInterData := make([][]KeyValue, reduceNum)
	for i := range tmpInterData {
		tmpInterData[i] = make([]KeyValue, reduceNum)
	}

	//将键值对写入到缓冲区中
	for _, keyVal := range mapInterData {
		reduceId := ihash(keyVal.Key) % reduceNum
		tmpInterData[reduceId] = append(tmpInterData[reduceId], keyVal) //添加到缓冲区中
	}

	var outFileNames []string

	for _, tmpKeyVals := range tmpInterData {

		if len(tmpKeyVals) > reduceNum {

			tmpKeyVals = tmpKeyVals[reduceNum:] //从第reduceNum开始为第一个元素
			reduceId := ihash(tmpKeyVals[0].Key) % reduceNum
			outFileName := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId) //拼接输出的文件名 mr-X-Y
			outFile, _ := os.OpenFile(outFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			enc := json.NewEncoder(outFile)

			//一次写入
			err := enc.Encode(tmpKeyVals) //写入到中间文件中
			if err != nil {
				fmt.Printf("write wrong!\n")

				return false, outFileNames
			}

			//fmt.Printf("reduceId: %v num: %v\n",reduceId,len(tmpKeyVals))
			outFile.Close()
			outFileNames = append(outFileNames, outFileName)
		}
	}

	return true, outFileNames
}

//reducer的任务详情
func ReduceWork(reducef func(string, []string) string, reduceId int, inputFileNames []string) bool {

	var kva []KeyValue //保存从文件中读入的kv值

	//读取所有reduceId匹配的文件
	for _, inputFileName := range inputFileNames {

		inputFile, err := os.OpenFile(inputFileName, os.O_RDONLY, 0777)

		if err != nil {
			fmt.Printf("open mr-*-%v file failed\n", reduceId)
		}

		//成批读入
		dec := json.NewDecoder(inputFile)

		for {
			var tmpKv []KeyValue
			if err := dec.Decode(&tmpKv); err != nil {
				break
			}
			kva = append(kva, tmpKv...)
		}

		inputFile.Close() //关闭输入文件

	}

	//排序，是key值按照从小到大的顺序
	sort.Sort(ByKey(kva))

	//reduce操作后输出到不同的oname文件中去
	oname := "mr-out-" + strconv.Itoa(reduceId)
	ofile, err := ioutil.TempFile("./", "mr-out-tmp*") //先写道tmp文件中

	if err != nil {
		fmt.Printf("open tmp file failed\n")
		return false
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	os.Rename(ofile.Name(), oname) //Reduce操作，完成之后修改名字

	//删除中间文件
	for _, inputFileName := range inputFileNames {
		os.Remove(inputFileName)
	}

	fmt.Printf("reduce task %v finished \n", reduceId)

	CallForReduceFinishReply(reduceId) //通知master,编号为reduceId的任务已完成

	return true
}


//
// main/mrworker.go calls this function.
//总体的worker调度函数
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {


	var sleepTime int = 5		//睡眠时间
	//执行map任务
	for {

		fileName, taskId, ok := CallForMapTask() //向master申请一个任务

		if ok { //返回成功代表，Map任务还未完全处理完毕
			MapWorker(mapf, fileName, taskId) //开启map进程,传入要操作的文件名
			time.Sleep(time.Second * time.Duration(sleepTime))

		} else {		//所有的map任务已分派，但是还未完成

			isAllMapTaskFinished := CallForAllMapFinishReply()
			if isAllMapTaskFinished { //如果全部Map任务都结束了，跳出循环继续执行Reduce任务，否则继续循环
				break
			}else{
				fmt.Printf("map:%v sleep for a while\n",taskId)
				time.Sleep(time.Second * time.Duration(sleepTime)) //任务分配完毕，休息一会儿等待任务执行完毕
			}
		}
	}

	fmt.Printf("all map tasks finished!\n")
	fmt.Printf("reduce tasks begin...\n")
	//此处开始，可以执行reduce操作

	for {
		reduceId, inputFileNames, ok := CallForReduceTask() //向master请求Reduce任务

		if ok {		//返回成功代表reduce还未分配结束

			fmt.Printf("ReduceId:%v running....\n", reduceId)
			ReduceWork(reducef, reduceId, inputFileNames)
			time.Sleep(time.Second * time.Duration(sleepTime))

		} else {		//返回失败，代表所有的任务都已经分配结束，但是还未完成
			isFinished := CallForAllReduceFinishReply() //判断是否全部Reduce任务结束
			if isFinished {
				break
			}else{
				fmt.Printf("reduce:%v sleep for a while\n",reduceId)
				time.Sleep(time.Second * time.Duration(sleepTime)) //任务分配完毕，休息一会儿等待任务执行完毕

			}
		}
	}

	fmt.Printf("all reduce tasks finished!\n")
	CallAllTaskFinished() //通知master，所有任务都已经完成

}


//////////////////////////下面是rpc请求//////////////////////////////////////////

//rpc请求一个split，这里返回一个文件名
func CallForMapTask() (string, int, bool) {
	pid := os.Getpid()
	args := CallForMapTaskArgs{pid}
	reply := CallForMapTaskReplyArgs{}

	call("Master.ReplyForMapTask", &args, &reply) //返回是否调用成功
	var ok bool = true
	if reply.FileName == "" { //如果FileName为空的话，说明任务处理完毕，返回false
		ok = false
	}

	return reply.FileName, reply.MapId, ok

}

//map任务结束给master返回消息
func CallForMapFinishReply(outFileNames []string, mapId int) bool {
	args := CallForMapFinishArgs{outFileNames, mapId}
	reply := CallForMapFinishReplyArgs{false}

	call("Master.ReplyForMapFinish", &args, &reply)

	return reply.Ok
}

//map任务结束给master返回消息
func CallForAllMapFinishReply() bool {
	args := CallForAllMapFinishArgs{}
	reply := CallForAllMapFinishReplyArgs{}

	call("Master.ReplyForAllMapFinish", &args, &reply)

	return reply.IsFinished
}

//请求Reduce任务
func CallForReduceTask() (int, []string, bool) {

	args := CallForReduceTaskArgs{}       //把ReduceId作为参数
	reply := CallForReduceTaskReplyArgs{} //返回所有mr-*-ReduceI的中间文件

	call("Master.ReplyForReduceTask", &args, &reply) //返回是否调用成功

	return reply.ReduceId, reply.ReduceFileNames, reply.Ok
}

//回复Master，编号为reduceId的任务已完成
func CallForReduceFinishReply(reduceId int) bool {
	args := CallForReduceFinishArgs{reduceId}
	reply := CallForReduceFinishReplyArgs{}

	call("Master.ReplyForReduceFinish", &args, &reply)

	return reply.OK //返回是否请求成功
}

func CallForAllReduceFinishReply() bool {
	args := CallForAllReduceFinishArgs{}
	reply := CallForAllReduceFinishReplyArgs{}

	call("Master.ReplyForAllReduceFinish", &args, &reply)

	return reply.IsFinished
}

//通知Master所有任务均已完成
func CallAllTaskFinished() {
	args := CallForAllTaskFinishArgs{}
	reply := CallForAllTaskFinishReplyArgs{}

	call("Master.ReplyForAllTaskFinish", &args, &reply)

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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