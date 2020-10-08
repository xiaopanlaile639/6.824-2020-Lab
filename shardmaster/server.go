package shardmaster

import (
	"../raft"
)
import "../labrpc"
import "sync"
import "../labgob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
	// Your data here.
	//curConfigNum	int		//当前的configuration编号
	//lastConfigIndex	int		//应用的最后一个Index(已经确认的)

	lastConfigIndexMap map[int64]int             //跟踪每个client最后执行的一个编号
	clientChannels     map[RequestId]chan Result //判断configIndex的命令是否完成
}


type Op struct {
	// Your data here.
	Method        string //调用的方法
	Configuration Config //传递配置
	ClientId	int64
	ConfigIndex	int

	Args []string

	DisJoinArgs JoinArgs
	DisLeaveArgs LeaveArgs
	DisMoveArgs	MoveArgs
	DisQueryArgs QueryArgs


}

//初始化new config
func (sm *ShardMaster) InitNewConfig(newConfig * Config){

	//lastCfg := sm.configs[len(sm.configs)-1]
	//nextCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: make(map[int][]string)}
	//for gid, servers := range lastCfg.Groups {
	//	nextCfg.Groups[gid] = append([]string{}, servers...)
	//}
	//	return nextCfg

	lastConfig:=sm.GetLastConfig()
	newConfig.Num = lastConfig.Num+1
	newConfig.Shards = lastConfig.Shards
	newConfig.Groups = make(map[int][]string)

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}

	//curConfigNum:=len(sm.configs)
	//newConfig.Num = curConfigNum
	//newConfig.Groups = nil
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	 sendOp:=Op{
		 Method:        JoinConfig,
		// Configuration: curConfig,
		 ClientId:      args.ClientId,
		 ConfigIndex:   args.ConfigIndex,
		 DisJoinArgs: *args,
	 }

	 res:=sm.SendConfigToRaft(sendOp)

	 reply.WrongLeader = res.WrongLeader
	 reply.Err = res.Err


	 sm.mu.Lock()
	 DPrintf("%v after join(%v)\n",args.Servers,reply.Err)
	 sm.ShowCurGroup()
	 sm.mu.Unlock()
}



func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.


	sendOp:=Op{
		Method:       LeaveConfig,
	//	Configuration: newConfig,
		ClientId:      args.ClientId,
		ConfigIndex:   args.ConfigIndex,
		DisLeaveArgs: *args,
	}

	//发送给底层raft系统
	res:=sm.SendConfigToRaft(sendOp)

	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err

	sm.mu.Lock()
	DPrintf("%v after leave(%v)\n",args.GIDs,reply.Err)
	sm.ShowCurGroup()
	sm.mu.Unlock()


}


func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	sendOp:=Op{
		Method:        MoveConfig,
		//Configuration: newConfig,
		ClientId:      args.ClientId,
		ConfigIndex:   args.ConfigIndex,
		DisMoveArgs: *args,
	}

	res:=sm.SendConfigToRaft(sendOp)

	reply.WrongLeader = res.WrongLeader
	reply.Err = res.Err


}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	sendOp:=Op{
		Method:        QueryConfig,
		//Configuration: Config{},
		ClientId:      args.ClientId,
		ConfigIndex:   args.ConfigIndex,
		//Args: argStr,			//QueryConfig带有一个参数
		DisQueryArgs: *args,
	}

	res:=sm.SendConfigToRaft(sendOp)

	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.RetConfig			//只有query消息需要这个消息

}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {

	//SetDebug()

	sm := new(ShardMaster)
	sm.me = me

	//sm.configs = make([]Config)
	//sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	DPrintf("%v start shardmaster\n",me)
	firstConfig:=Config{
		Num:    0,
		Shards: [10]int{},
		Groups: nil,
	}
	//插入第一个config
	sm.configs = append(sm.configs,firstConfig)
	//sm.lastConfigIndex = 0

	sm.lastConfigIndexMap = make(map[int64]int)         //初始化lastCmdIndex
	sm.clientChannels = make(map[RequestId]chan Result) //初始化

	go sm.RecConfigMsg()			//在单独的一个go 线程中 接收信息
	return sm
}


//func SetDebug(){
//
//	if len(os.Args)>1{
//		Debug1,_=strconv.Atoi(os.Args[1])
//	}else{
//		Debug1 = 0
//	}
//
//}