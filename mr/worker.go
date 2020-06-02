package mr

import (
	"encoding/json"
	"sort"

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

type IntermidData struct {
	Intermediate []KeyValue
	InterLock sync.Mutex		//保护中间数据的锁
}

type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


var mapWriteLocks  [10]sync.Mutex		//保护中间数据的锁
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
func MapWorker(mapf func(string, string) []KeyValue, fileName string, taskId int,waitgroup *sync.WaitGroup) error {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kvRet := mapf(fileName, string(content)) //用户提供的Map函数执行结束的键值对

	////将kv写入文件，并通知master///

	//互斥写入到中间缓冲区中去
	//interData.InterLock.Lock()
	//interData.Intermediate = append(interData.Intermediate,kvRet...) //添加中间数据

	_,outFileNames:=WriteToIntFile(kvRet,taskId)			//写入到中间文件中去

	fmt.Printf("task %v finished.\n",taskId)

	CallForMapFinishReply(outFileNames,taskId)			//返回给master写入的文件名信息

	//interData.InterLock.Unlock()

	waitgroup.Done()		//wait数目减少一个


	return nil

	//CallForMapFinishReply(outFileNames,mapId)		//返回写入的中间文件名和mapId
}

//写入到中间文件中去
func WriteToIntFile( mapInterData []KeyValue, mapId int)(bool,[]string){

	reduceNum := 10			//默认reducer的数量为10

	//初始化一个二维切片
	tmpInterData:= make([][]KeyValue,reduceNum)
	for i:= range tmpInterData {
		tmpInterData[i] = make([]KeyValue,reduceNum)
	}


	//将键值对写入到缓冲区中
	for _,keyVal:= range mapInterData {
		reduceId := ihash(keyVal.Key)%reduceNum
		tmpInterData[reduceId] = append(tmpInterData[reduceId], keyVal)		//添加到缓冲区中


	}


	var outFileNames []string

	for _,tmpKeyVals := range tmpInterData {

		if len(tmpKeyVals) > reduceNum {

			tmpKeyVals = tmpKeyVals[reduceNum:]		//从第reduceNum开始为第一个元素

			reduceId := ihash(tmpKeyVals[0].Key)%reduceNum

		//	mapWriteLocks[reduceId].Lock()		//加reduceId对应的文件锁

			outFileName:= "mr-"+ strconv.Itoa(mapId)+"-"+strconv.Itoa(reduceId)		//拼接输出的文件名 mr-X-Y

			outFile,_:=os.OpenFile(outFileName,os.O_WRONLY | os.O_CREATE|os.O_APPEND,0666)
			enc := json.NewEncoder(outFile)


			//一次写入
			err :=enc.Encode(tmpKeyVals)		//写入到中间文件中
			if err != nil{
				fmt.Printf("write wrong!\n")

				//mapWriteLocks[reduceId].Unlock()	//解锁

				return false,outFileNames
			}


			fmt.Printf("reduceId: %v num: %v\n",reduceId,len(tmpKeyVals))

			outFile.Close()

			//	mapWriteLocks[reduceId].Unlock()	//解锁

			outFileNames = append(outFileNames, outFileName)
		}

	}


	return true,outFileNames
}

func ReduceWork(reducef func(string, []string) string,reduceId int,waitgroup *sync.WaitGroup)bool {

	inputFileNames,ok:=CallForReduceTask(reduceId)		//向master请求任务

	if !ok {
		fmt.Printf("Call for reduce task %v failed\n",reduceId)
		return false
	}

	var kva []KeyValue //保存从文件中读入的kv值

	//从文件中读取json格式的数据
//	inputFileName := "mr-"+strconv.Itoa(reduceId)

	//读取所有reduceId匹配的文件
	for _,inputFileName := range inputFileNames {

		inputFile,err:=os.OpenFile(inputFileName,os.O_RDONLY,0666)

		if err != nil {
			fmt.Printf("open mr-*-%v file failed\n",reduceId)
		}

		//成批读入
		dec:= json.NewDecoder(inputFile)

		for{
			var tmpKv []KeyValue
			if err := dec.Decode(&tmpKv); err !=nil {
				break
			}

			kva=append(kva, tmpKv...)
		}

		fmt.Printf("reduceId1: %v num: %v\n",reduceId,len(kva))

		inputFile.Close()		//关闭输入文件
	}


	sort.Sort(ByKey(kva))

	//reduce操作后输出到不同的oname文件中去
	oname := "mr-out-"+strconv.Itoa(reduceId+1)

	ofile, _ := os.OpenFile(oname,os.O_WRONLY | os.O_CREATE|os.O_APPEND,0666)
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

	fmt.Printf("reduce task %v finished \n",reduceId)

	waitgroup.Done()		//wait数目减1

	return true
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	 //CallExample()

	var waitgroup sync.WaitGroup
	//
	for mapf != nil {			//map任务

		fileName,taskId,ok:= CallForMapTask()		//向master申请一个任务

		if ok {

			fmt.Printf("dealing fileName:%v,mapId:%v....\n", fileName, taskId)

			waitgroup.Add(1)
			go MapWorker(mapf, fileName, taskId,&waitgroup) //开启map进程,传入要操作的文件名

			//time.Sleep(time.Second ) //休眠1秒钟
		} else {
			break
		}
	}

	waitgroup.Wait()

	fmt.Printf("map tasks finished!\n")

	fmt.Printf("reduce tasks begin...\n")
	//此处开始，可以执行reduce操作
	for i:=0;i<10;i++ {
		waitgroup.Add(1)
		go ReduceWork(reducef,i,&waitgroup)		//执行reduce任务
	}
	waitgroup.Wait()

	fmt.Printf("all reduce tasks finished!\n")

}

//map任务结束给master返回消息
func CallForMapFinishReply(outFileNames []string, mapId int) bool{
	args:= CallForMapFinishArgs{outFileNames,mapId}
	reply := CallForMapFinishReplyArgs{false}

	call("Master.ReplyForMapFinish", &args, &reply)

	return reply.Ok
}

//rpc请求一个split，这里返回一个文件名
func CallForMapTask() (string,int,bool) {

	args:= CallForMapTaskArgs{0}

	reply := CallForMapTaskReplyArgs{}

	call("Master.ReplyForMapTask", &args, &reply)		//返回是否调用成功

	var ok bool = true

	if reply.FileName =="" {			//如果FileName为空的话，说明任务处理完毕，返回false
		ok=false

		fmt.Printf("all map tasks has been allocated %v\n", reply.FileName)
	}else {
		fmt.Printf("reply file name: %v\n", reply.FileName)

	}

	return reply.FileName,reply.TaskId,ok

}

//请求Reduce任务
func CallForReduceTask(ReduceId int) ([]string,bool){
	args:= CallForReduceTaskArgs{ReduceId}		//把ReduceId作为参数
	reply:=CallForReduceTaskReplyArgs{}					//返回所有mr-*-ReduceI的中间文件

	call("Master.ReplyForReduceTask", &args, &reply)		//返回是否调用成功

	return reply.ReduceFileNames,reply.Ok
}


//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
