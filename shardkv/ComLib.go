package shardkv

import (
	"../labgob"
	"../shardmaster"
	"bytes"
	"log"
	"os"
	"time"
)

const(
	TimeOut	   = "TimeOut"
	NotOK		   = "NotOK"
)

const(
	Get = "Get"
	PUtAppend = "PutAppend"
	Put = "Put"
	Append = "Append"
	SendConfig = "SendConfig"
	SendShard = "SendShard"
)

//shard从哪到哪
type ChaShardPair struct{
	fromGid int
	toGid int
	shard int 		//shard编号
}

type ChaArgs struct {
	ChaDB	map[string]string			//交换的数据

}
type ChaReply struct {
	Err Err

}

//返回结果
type Result struct {
	Err Err
	RetVal string
}

//每个request的标识
type  RequestId struct {
	ClientId	int64
	CmdIndex	int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Method string			//调用的方法
	MethodArgs []string		//调用的参数

	ClientId	int64		//请求的客户端id
	CmdIndex	int			//指令的编号

	//NewConfig shardmaster.Config
	//
	//ChaDB	map[string]string	//交换的DB

}

const Debug = 1
//const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}



func (kv * ShardKV) SendCmdToRaft(clientId int64, cmdIndex int, op Op)Result{

	retRes:=Result{
		Err: ErrWrongLeader,
		RetVal: "",
	}

	if _, isLeader:=kv.rf.GetState(); isLeader{

		DPrintf("%v(%v) send %v to raft, cmdIndex is %v\n",kv.me,kv.gid,op,cmdIndex)
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
func (kv *ShardKV) ApplyCmdToDB(request Op, commitIndex int,commitTerm int)Result{

	res := Result{}
	res.Err = OK

	kv.mu.Lock()
	defer  kv.mu.Unlock()

	switch request.Method {
	case Get:
		key := request.MethodArgs[0]
		val,ok:=kv.DataBase[key]
		if ok {
			res.RetVal = val
		}else {
			res.Err = ErrNoKey
		}

		break
	case Put:
		lastCmdIndex,ok:=kv.lastCmdIndexMap[request.ClientId]
		if !ok || (ok && lastCmdIndex <request.CmdIndex) {			//不存在或者(存在并且当前request的编号较大)
			key := request.MethodArgs[0]
			val := request.MethodArgs[1]
			kv.DataBase[key]  = val		//key不存在就插入，存在就覆盖其val

			kv.lastCmdIndexMap[request.ClientId] = request.CmdIndex	//apply 到DB中完成，记录新的编号

			//DPrintf("after %v apply %v, it is %v now\n",kv.me,request,kv.DataBase[key])
		}

		break
	case Append:
		lastCmdIndex,ok:=kv.lastCmdIndexMap[request.ClientId]
		if !ok || (ok && lastCmdIndex <request.CmdIndex) {
			key := request.MethodArgs[0]
			val := request.MethodArgs[1]

			if retVal,ok:=kv.DataBase[key]; ok{		//存在对应的key
				kv.DataBase[key]  = retVal + val		//append在arg之后
			}else{
				kv.DataBase[key]  = val		//key不存在就插入，存在就覆盖其val
			}

			kv.lastCmdIndexMap[request.ClientId] = request.CmdIndex	//apply 到DB中完成，记录新的编号

			//DPrintf("after %v apply %v, it is %v now\n",kv.me,request,kv.DataBase[key])

		}

		break
	}

	kv.ShowDB()

	kv.LastAppliedIndex = commitIndex			//记录最后一个应用到DB的log编号
	kv.LastAppliedTerm = commitTerm				//记录最后一个应用到DB的log所在的term
	return res
}

//专门接受raft系统的返回信息，通过通道
func (kv *ShardKV) RecApplyMsg(){

	for {
		AppMsg, _ := <-kv.applyCh //接受:

		DPrintf("%v(%v) rev AppMsg:%v \n", kv.me, kv.gid, AppMsg)

		if AppMsg.CommandValid { //如果是普通apply 消息

			if cfg, ok := AppMsg.Command.(shardmaster.Config); ok { //如果是config消息
				kv.UpdateInAndOutDataShard(cfg) //更新shard相关信息
			} else if migrationData, ok := AppMsg.Command.(MigrateReply); ok { //如果是数据消息
				kv.UpdateDBWithMigrateData(migrationData)
			} else if op := AppMsg.Command.(Op); ok { //如果接口类型强制转化为Op类型

				res := kv.ApplyCmdToDB(op, AppMsg.CommandIndex, AppMsg.CommandTerm) //将操作具体应用到DB中

				if _, isLeader := kv.rf.GetState(); isLeader {
					reqId := RequestId{
						ClientId: op.ClientId,
						CmdIndex: op.CmdIndex,
					}
					ok, ch := kv.GetClientChannel(reqId)
					if ok {
						ch <- res
						kv.RemoveClientChannel(reqId) //删除通道，说明此任务已完成

					} else {
						DPrintf("error : could not find notify channel\n")
					}
				}

				go kv.SaveSnapshot() //保存快照

			} else { //snapshot 消息
				kv.ReadSnapshot() //恢复快照
			}
		}
	}
}

//处理snapshot,若snapshot达到限额
func (kv *ShardKV) SaveSnapshot(){

	kv.mu.Lock()

	raftLogSize:=kv.rf.GetRaftStateSize()
	if kv.maxraftstate != -1 && raftLogSize >= kv.maxraftstate{

		DPrintf("%v's log size(%v) reach to maxsize(%v)\n",kv.me,raftLogSize,kv.maxraftstate)

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.DataBase)
		e.Encode(kv.lastCmdIndexMap)
		e.Encode(kv.comeInShards)
		e.Encode(kv.toOutShards)
		e.Encode(kv.myShards)
		e.Encode(kv.curConfig)
		//e.Encode(kv.garbages)

		kv.rf.SaveStateAndSnapshotByte(kv.LastAppliedIndex,kv.LastAppliedTerm,w.Bytes())
	}

	kv.mu.Unlock()

}

func (kv* ShardKV) ReadSnapshot()bool{


	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshotByte:=kv.rf.ReadSnapshot()
	if snapshotByte == nil || len(snapshotByte) < 1{
		return false
	}

	r := bytes.NewBuffer(snapshotByte)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var myShards	map[int]bool
	var cfg shardmaster.Config

	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil || d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil || d.Decode(&myShards) != nil || d.Decode(&cfg) != nil {
		log.Fatalf("readSnapShot ERROR for server %v", kv.me)
		return false
	} else {
		kv.DataBase, kv.lastCmdIndexMap, kv.curConfig = db, cid2Seq, cfg
		kv.toOutShards, kv.comeInShards, kv.myShards = toOutShards,comeInShards,myShards
	}

	DPrintf("%v read snapshot,it 's lastIncIndex:%v",kv.me,kv.LastAppliedIndex)
	return true
}


//获得通道
func (kv* ShardKV) GetClientChannel(reqId RequestId) (bool,chan Result) {
	var ok bool
	var ch chan Result
	kv.mu.Lock()
	ch,ok = kv.clientChannels[reqId]
	kv.mu.Unlock()
	return ok,ch
}
//插入通道
func (kv* ShardKV) InsertClientChannel(reqId RequestId,ch chan Result) {
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
func (kv* ShardKV) RemoveClientChannel(reqId RequestId) {
	kv.mu.Lock()
	delete(kv.clientChannels,reqId)
	kv.mu.Unlock()
}

