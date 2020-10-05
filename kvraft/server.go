package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"

	//"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

)

//import "../comprit"


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//type AppInfo struct {
//	Term int
//	Index	int
//}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Method string			//调用的方法
	MethodArgs []string		//调用的参数

	ClientId	int64		//请求的客户端id
	OpIndex	int64			//指令的编号
}

//返回结果
type Result struct {
	Err Err
	RetVal string
}

//每个request的标识
type  RequestId struct {
	ClientId	int64
	CmdIndex	int64
}



type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DataBase	map[string]string			//kv键值数据库
	lastCmdIndexMap	map[int64]int		//跟踪每个client最后执行的一个编号

	clientChannels map[RequestId]chan Result //判断cmdIndex的命令是否完成

	LastAppliedIndex int
	LastAppliedTerm int

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//构造传给Raft的命令和参数
	methodArgs := []string{args.Key}		//只有一个key参数
	op := Op{
		Method:     "Get",
		MethodArgs: methodArgs,
		OpIndex: args.CmdIndex,			//指令编号
		ClientId: args.ClientId,

	}

	retRes:=Result{
		Err:    ErrWrongLeader,
		RetVal: "",	
	}

	if _,isLeader:=kv.rf.GetState(); isLeader{		//只有是leader才能发送命令
		retRes=kv.SendCmd(args.ClientId,args.CmdIndex,op)		//发送执行命令
	}

	reply.Err = retRes.Err
	reply.Value = retRes.RetVal


}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	methodArgs := []string{args.Key,args.Value}			//key,value两个参数
	op:=Op{
		Method:     args.Op,	 		//put or append
		MethodArgs: methodArgs,
		OpIndex: args.CmdIndex,				//命令编号
		ClientId: args.ClientId,
	}

	retRes:=Result{
		Err:    ErrWrongLeader,
		RetVal: "",
	}

	if _,isLeader:=kv.rf.GetState(); isLeader{			//只有是leader才能发送命令
		retRes=kv.SendCmd(args.ClientId,args.CmdIndex,op)			//发送命令
	}

	reply.Err = retRes.Err

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}



//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	DPrintf("Init KVServer(%v)...\n",me)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.DataBase = make(map[string]string)      //初始化kv数据库
	kv.lastCmdIndexMap = make(map[int64]int) //初始化lastCmdIndex
	kv.clientChannels = make(map[RequestId]chan Result)  //初始化

	kv.LastAppliedIndex = -1
	kv.LastAppliedTerm = -1

	kv.ReadSnapshot()			//首先从快照中恢复

	go kv.RecApplyChan()				//单独开一个线程用于 读取从raft系统通道里返回的数据

	return kv
}


func (kv * KVServer) SendCmd(clientId int64, cmdIndex int64, op Op)Result{

	retRes:=Result{
		Err: ErrWrongLeader,
		RetVal: "",
	}
	if _, isLeader:=kv.rf.GetState(); isLeader{

		DPrintf("%v send %v to raft, cmdIndex is %v\n",kv.me,op,cmdIndex)
		kv.rf.Start(op)

		ch := make(chan Result)
		reqId := RequestId{
			ClientId: clientId,
			CmdIndex: cmdIndex,
		}
		kv.InsertClientChannel(reqId,ch)

		//等待回复或者超时
		select{
		case <- time.After(time.Second):
			DPrintf("%v time out\n",kv.me)
			retRes.Err = TimeOut
			retRes.RetVal=""
			kv.RemoveClientChannel(reqId)			//超时后删除通道

		case res,ok:=<-ch:
			if ok{
				retRes.Err = res.Err
				retRes.RetVal = res.RetVal
				kv.RemoveClientChannel(reqId)			//返回成功后也要删除通道
			}else{			//如果ch通道已经被删除，说明请求已经被处理了
				DPrintf("server is not leader now")
				retRes.Err = ErrWrongLeader
			}
		}

	}else{
		retRes.Err = ErrWrongLeader
		DPrintf("I(%v) am not the leader\n",kv.me)
	}

	return retRes
}


//把命令应用到DB中
func (kv *KVServer) ApplyCmdToDB(request Op, commitIndex int,commitTerm int)Result{

	res := Result{}
	res.Err = OK

	kv.mu.Lock()
    defer  kv.mu.Unlock()

	switch request.Method {
	case "Get":
		key := request.MethodArgs[0]
		val,ok:=kv.DataBase[key]
		if ok {
			res.RetVal = val
		}else {
			res.Err = ErrNoKey
		}

		break
	case "Put":
		lastCmdIndex,ok:=kv.lastCmdIndexMap[request.ClientId]
		if !ok || (ok && lastCmdIndex <request.OpIndex) {			//不存在或者(存在并且当前request的编号较大)
			key := request.MethodArgs[0]
			val := request.MethodArgs[1]
			kv.DataBase[key]  = val		//key不存在就插入，存在就覆盖其val

			kv.lastCmdIndexMap[request.ClientId] = request.OpIndex	//apply 到DB中完成，记录新的编号

			DPrintf("after %v apply %v, it is %v now\n",kv.me,request,kv.DataBase[key])
		}

		break
	case "Append":
		lastCmdIndex,ok:=kv.lastCmdIndexMap[request.ClientId]
		if !ok || (ok && lastCmdIndex <request.OpIndex) {
			key := request.MethodArgs[0]
			val := request.MethodArgs[1]

			if retVal,ok:=kv.DataBase[key]; ok{		//存在对应的key
				kv.DataBase[key]  = retVal + val		//append在arg之后
			}else{
				kv.DataBase[key]  = val		//key不存在就插入，存在就覆盖其val
			}

			kv.lastCmdIndexMap[request.ClientId] = request.OpIndex	//apply 到DB中完成，记录新的编号

			DPrintf("after %v apply %v, it is %v now\n",kv.me,request,kv.DataBase[key])

		}

		break
	}

	kv.LastAppliedIndex = commitIndex			//记录最后一个应用到DB的log编号
	kv.LastAppliedTerm = commitTerm				//记录最后一个应用到DB的log所在的term
	return res
}


//专门接受raft系统的返回信息，通过通道
func (kv *KVServer) RecApplyChan(){

	for{
		 AppMsg,_:= <- kv.applyCh    //接受:

		DPrintf("%v rev AppMsg:%v \n",kv.me,AppMsg)

		if AppMsg.CommandValid { //如果是普通apply 消息

			op:=AppMsg.Command.(Op)			//接口类型强制转化为Op类型
			res := kv.ApplyCmdToDB(op,AppMsg.CommandIndex,AppMsg.CommandTerm) //将操作具体应用到DB中

			if _, isLeader := kv.rf.GetState(); isLeader {
				reqId:=RequestId{
					ClientId: op.ClientId,
					CmdIndex: op.OpIndex,
				}
				ok,ch:=kv.GetClientChannel(reqId)
				if ok{
					ch<-res
					kv.RemoveClientChannel(reqId)			//删除通道，说明此任务已完成

				}else {
					DPrintf("error : could not find notify channel\n")
				}
			}
			go kv.SaveSnapshot()			//保存快照

		}else{				//snapshot 消息
			kv.ReadSnapshot()		//恢复快照

		}
	}
}


//处理snapshot,若snapshot达到限额
func (kv *KVServer) SaveSnapshot(){

	kv.mu.Lock()

	raftLogSize:=kv.rf.GetRaftStateSize()
	if kv.maxraftstate != -1 && raftLogSize >= kv.maxraftstate{


		DPrintf("%v's log size(%v) reach to maxsize(%v)\n",kv.me,raftLogSize,kv.maxraftstate)

		snapShot:=raft.SnapShot{
			DB:              kv.DataBase,
			LastCmdIndexMap: kv.lastCmdIndexMap,
			LastAppliedIndex: kv.LastAppliedIndex,
			LastAppliedTerm: kv.LastAppliedTerm,
		}
		kv.rf.SaveStateAndSnapshot(snapShot)
	}
	kv.mu.Unlock()

}

func (kv* KVServer) ReadSnapshot()bool{

	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshotByte:=kv.rf.ReadSnapshot()

	if snapshotByte == nil || len(snapshotByte) < 1{
		return false
	}

	snapshotInt,_:=kv.rf.Deserialize(snapshotByte,raft.SnapshotType)
	snapshot := snapshotInt.(raft.SnapShot)


	kv.DataBase = snapshot.DB
	kv.lastCmdIndexMap = snapshot.LastCmdIndexMap
	kv.LastAppliedIndex = snapshot.LastAppliedIndex
	kv.LastAppliedTerm = snapshot.LastAppliedTerm

	DPrintf("%v read snapshot,it 's lastIncIndex:%v",kv.me,kv.LastAppliedIndex)
	return true
}


//更新上一条命令
func (kv *KVServer) UpdateLastCmdIndex(clientId int64, cmdIndex int64){

	kv.mu.Lock()
	kv.lastCmdIndexMap[clientId] = cmdIndex		//更新命令编号
	kv.mu.Unlock()

}


//返回KV数据库中key所对应的的val，失败返回“”
func (kv* KVServer) GetValByKey(key string)string{
	retVal:=""
	kv.mu.Lock()
	if val,ok:=kv.DataBase[key];ok{
		retVal = val
	}
	kv.mu.Unlock()

	return retVal
}

//获得通道
func (kv* KVServer) GetClientChannel(reqId RequestId) (bool,chan Result) {
	var ok bool
	var ch chan Result
	kv.mu.Lock()
	ch,ok = kv.clientChannels[reqId]
	kv.mu.Unlock()
	return ok,ch
}
//插入通道
func (kv* KVServer) InsertClientChannel(reqId RequestId,ch chan Result) {
	kv.mu.Lock()
	_,ok := kv.clientChannels[reqId]
	if ok {

		DPrintf("%v insert ch for reqId(%v) error : ch exits \n",kv.me,reqId)
		os.Exit(-1)
	}
	kv.clientChannels[reqId] = ch
	kv.mu.Unlock()
}
//移走通道
func (kv* KVServer) RemoveClientChannel(reqId RequestId) {
	kv.mu.Lock()
	delete(kv.clientChannels,reqId)
	kv.mu.Unlock()
}

