package shardmaster

import (
	"log"
	"os"
	"time"
)

//返回结果
type Result struct {
	WrongLeader bool
	Err         Err

	RetConfig	Config
}

//每个request的标识
type  RequestId struct {
	ClientId	int64
	ConfigIndex	int
}

const Debug = 1
//const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


//把从raft中传回的config应用到ShardMaster
func (sm *ShardMaster) ApplyConfigToSM(request Op)Result{

	res := Result{}
	res.Err = OK
	res.WrongLeader = false

	sm.mu.Lock()
	defer  sm.mu.Unlock()

	curConfig:=Config{}
	//填充num编号信息
	sm.InitNewConfig(&curConfig)

	DPrintf("in Apply Config: %v's curConfig Num is %v\n",sm.me,curConfig.Num)

	//获得new group 信息,
	lastServers:=sm.GetLastServers()
	lastCopyServers:=DeepCopyMap(lastServers)

	configOkFlag :=false

	_lastConfigIndex,ok:=sm.lastConfigIndexMap[request.ClientId]
	if !ok || (ok && _lastConfigIndex < request.ConfigIndex) { //不存在或者(存在并且当前request的编号较大)

		switch request.Method {
		case JoinConfig:
			joinArgs:= request.DisJoinArgs

			newCurSers:=sm.JoinServers(lastCopyServers,joinArgs.Servers)			//当前的servers

			curConfig.Groups = newCurSers

			//填充 new config 的shard信息
			sm.AllocateShard(&curConfig)

			configOkFlag = true
			break
		case LeaveConfig:
			leaveArgs:=request.DisLeaveArgs
			newCurSers:=sm.LeaveServers(lastCopyServers,leaveArgs.GIDs)

			curConfig.Groups = newCurSers

			//填充 new config 的shard信息
			sm.AllocateShard(&curConfig)

			configOkFlag = true
			break

		case MoveConfig:
			moveArgs:=request.DisMoveArgs

			lastConfig:=sm.GetLastConfig()
			//转移
			lastConfig.Shards[moveArgs.Shard] = moveArgs.GID

			curConfig.Groups = lastServers		//server group 不变
			curConfig.Shards = lastConfig.Shards	//更换为新的shards

			configOkFlag = true
			break
		case QueryConfig:					//如果是query消息
			queryConfig:=request.DisQueryArgs
			num:=queryConfig.Num

			maxNumConfig:=sm.GetLastConfig().Num

			if num ==  -1 || num >=maxNumConfig{
				res.RetConfig = sm.configs[maxNumConfig]
				res.Err = OK
				res.WrongLeader = false
			}else if  num >= 0 && num < maxNumConfig {
				res.RetConfig = sm.configs[num]
				res.Err = OK
				res.WrongLeader = false
			}else {
				res.Err = NotOK
				res.WrongLeader = true
			}

			break
		}

		//添加进sm中
		if configOkFlag{			//需要添加至config
			sm.configs = append(sm.configs,curConfig)
			sm.lastConfigIndexMap[request.ClientId] = request.ConfigIndex

			res.Err = OK
			res.WrongLeader = false

			DPrintf("%v is in applyConfig function, curConfigNum is %v",sm.me,curConfig.Num)

		}
	}else{				//已经失效的请求
		res.Err = NotOK
		res.WrongLeader = true
	}

	return res
}


//专门通过通道接受raft系统的返回信息
func (sm *ShardMaster) RecConfigMsg(){

	for{
		AppMsg,_:= <- sm.applyCh    //接受:

		DPrintf("%v rev AppMsg:%v \n",sm.me,AppMsg)

		recOp:=AppMsg.Command.(Op)

		res:=sm.ApplyConfigToSM(recOp)		///???

		if _, isLeader := sm.rf.GetState(); isLeader {
			reqId:=RequestId{
				ClientId:    recOp.ClientId,
				ConfigIndex: recOp.ConfigIndex,
			}

			ok,ch:=sm.GetClientChannel(reqId)
			if ok{
				ch<-res
				sm.RemoveClientChannel(reqId)
			}else{
				DPrintf("error : could not find notify channel(%v)\n",reqId)
			}
		}
	}
}


//发送Config到底层Raft中
func (sm *ShardMaster) SendConfigToRaft( op Op)Result{

	retRes:=Result{
		Err: NotOK,
		WrongLeader: true,
	}

	if _, isLeader:=sm.rf.GetState(); isLeader{

		//DPrintf("%v send config(%v) to raft, configIndex is %v\n",sm.me,op,op.ConfigIndex)

		sm.rf.Start(op)

		ch := make(chan Result)

		reqId := RequestId{
			ClientId: op.ClientId,
			ConfigIndex: op.ConfigIndex,
		}

		sm.InsertClientChannel(reqId,ch)

		//等待回复或者超时
		select{
		case <- time.After(time.Second):
			DPrintf("%v time out\n",sm.me)
			retRes.Err =" TimeOut"
			retRes.WrongLeader=true

			sm.RemoveClientChannel(reqId)			//超时后删除通道

		case res,ok:=<-ch:
			if ok{
				retRes = res
				sm.RemoveClientChannel(reqId)			//返回成功后也要删除通道
			}else{			//如果ch通道已经被删除，说明请求已经被处理了
				DPrintf("server is not leader now")
				retRes.Err = "ErrWrongLeader"
				retRes.WrongLeader = true
			}
		}

	}else{
		retRes.Err = "ErrWrongLeader"
		retRes.WrongLeader = true
		DPrintf("I(%v) am not the leader\n",sm.me)
	}

	return retRes
}


//获取最后一个config 时的server
func (sm *ShardMaster) GetLastServers()map[int][]string{

	return sm.GetLastConfig().Groups
}

//获取最后一个config
func (sm *ShardMaster) GetLastConfig()Config{

	conLen:=len(sm.configs)

	if conLen > 0{
		return sm.configs[conLen-1]
	}else{
		DPrintf(" GetLastConfig wrong\n")
		return Config{
			Num:    0,
			Shards: [10]int{},
			Groups: nil,
		}
	}

}

//给每个configuration分配shard
func (sm *ShardMaster) AllocateShard(curConfig *Config){

	curGroupNum:= len(curConfig.Groups)

	if curGroupNum == 0{

		DPrintf("%v's curGroupNum is 0, return \n",sm.me)
		return
	}

	//DPrintf("%v's curGroupNum:%v\n",sm.me, curGroupNum)

	aveNum:= NShards / curGroupNum
	leftNum:= NShards % curGroupNum

	//当前配置的所有组号
	var curGroupIds[]int

	for groNum,_ := range curConfig.Groups{
		curGroupIds = append(curGroupIds,groNum)
	}

	k:=-1
	tmpCountNum :=-1

	//分配所有的shard给不同的组
	for shardNum:=0;shardNum<NShards;shardNum++{

		if tmpCountNum <= 0{
			tmpCountNum = aveNum
			k++

			//首先给前面的补上1
			if k < leftNum{
				tmpCountNum++
			}
		}

		curConfig.Shards[shardNum] = curGroupIds[k]
		tmpCountNum--
	}
}


//添加servers
func (sm *ShardMaster) JoinServers(lastCopySers map[int][]string, joinSers map[int][]string)map[int][]string{

	for k,v:=range joinSers{
		lastCopySers[k] = v
	}

	return lastCopySers
}

func (sm *ShardMaster) LeaveServers(lastCopySers map[int][]string, leaGids []int) map[int][]string {

	DPrintf("%v's group is (%v)",sm.me,lastCopySers)
	for _,leaGid := range leaGids{

		if _, ok := lastCopySers[leaGid]; ok {
			delete(lastCopySers, leaGid)
		}else{
			//wrong
			DPrintf("%v: leaGid(%v) is not in group servers\n",sm.me,leaGid)
		}
	}

	return lastCopySers
}


func DeepCopyMap(value map[int][]string) map[int][]string{
	newMap:=make(map[int][]string)

	for k,v:=range value{
		newMap[k] = v
	}
	return newMap
}



//获得通道
func (sm *ShardMaster) GetClientChannel(reqId RequestId) (bool,chan Result) {
	var ok bool
	var ch chan Result
	sm.mu.Lock()
	ch,ok = sm.clientChannels[reqId]
	sm.mu.Unlock()
	return ok,ch
}

//插入通道
func (sm *ShardMaster) InsertClientChannel(reqId RequestId,ch chan Result) {
	sm.mu.Lock()
	_,ok := sm.clientChannels[reqId]
	if ok {

		DPrintf("%v insert ch for reqId(%v) error : ch exits \n",sm.me,reqId)
		os.Exit(-1)
	}
	sm.clientChannels[reqId] = ch
	sm.mu.Unlock()
}

//移走通道
func (sm *ShardMaster) RemoveClientChannel(reqId RequestId) {
	sm.mu.Lock()
	delete(sm.clientChannels,reqId)
	sm.mu.Unlock()
}

func (sm *ShardMaster) ShowCurGroup(){

	//sm.mu.Lock()
//	defer sm.mu.Unlock()

	//lastServers:=sm.GetLastServers()
	lastConfig:=sm.GetLastConfig()
	DPrintf("%v's curConfig(%v) group is %v\n",sm.me,lastConfig.Num,lastConfig.Groups)

}

