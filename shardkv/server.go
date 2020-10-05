package shardkv


// import "../shardmaster"
import (
	"../labrpc"
	"../shardmaster"
)
import "../raft"
import "sync"
import "../labgob"



type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	mck       *shardmaster.Clerk			//构造一个shardmaster，为了和其通信
	curConfig   shardmaster.Config			//当前的configuration

	lastCmdIndexMap	map[int64]int		//跟踪每个client最后执行的一个命令编号
	clientChannels map[RequestId]chan Result //判断cmdIndex的命令是否完成

	LastAppliedIndex int
	LastAppliedTerm int
	DataBase	map[string]string			//kv键值数据库

	isChanging	bool


}


//判断是否是当前group负责的shard
func (kv *ShardKV)IsResForTheKey(key string)bool{

	kv.mu.Lock()
	defer kv.mu.Unlock()

	keyShardId:= key2shard(key)
	if kv.curConfig.Shards[keyShardId] == kv.gid{
		return true
	}else{
		return false
	}
}

func (kv *ShardKV) ShowDB(){

	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	for key,val:=range kv.DataBase{
		DPrintf("%v(%v):key--val:%v:%v",kv.me,kv.gid,key,val)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if kv.GetChaFlag() {
		reply.Err = NotOK
		DPrintf("%v(%v) is changing data,so Get not ok\n",kv.me,kv.gid)
		return
	}

	kv.mu.Lock()
	DPrintf("%v(%v)'s curConfig's Num is %v\n",kv.me,kv.gid,kv.curConfig.Num)
	kv.mu.Unlock()

	if kv.IsResForTheKey(args.Key) == false{
		reply.Err = ErrWrongGroup
		DPrintf("%v is not res to the key(%v)\n",kv.me,args.Key)
		return
	}

	//构造传给Raft的命令和参数
	methodArgs := []string{args.Key}		//只有一个key参数
	op := Op{
		Method:     Get,
		MethodArgs: methodArgs,
		CmdIndex: args.CmdIndex,			//指令编号
		ClientId: args.ClientId,

	}

	retRes:=Result{
		Err:    ErrWrongLeader,
		RetVal: "",
	}

	if _,isLeader:=kv.rf.GetState(); isLeader{		//只有是leader才能发送命令
		retRes=kv.SendCmdToRaft(args.ClientId,args.CmdIndex,op)		//发送执行命令

		DPrintf("%v(%v) get key(%v)-val(%v)\n",kv.me,kv.gid,args.Key,retRes.RetVal)
	}else{
		DPrintf("%v(%v) is not the leader,so Get not ok\n",kv.me,kv.gid)
	}

	reply.Err = retRes.Err
	reply.Value = retRes.RetVal

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	//如果正在交换数据，则停止服务
	if kv.GetChaFlag() {
		reply.Err = NotOK
		DPrintf("%v(%v) is changing data,so PutAppend not ok\n",kv.me,kv.gid)
		return
	}

	kv.mu.Lock()
	DPrintf("%v(%v)'s curConfig's Num is %v\n",kv.me,kv.gid,kv.curConfig.Num)
	kv.mu.Unlock()

	if kv.IsResForTheKey(args.Key) == false{
		reply.Err = ErrWrongGroup
		return
	}

	methodArgs := []string{args.Key,args.Value}			//key,value两个参数
	op:=Op{
		Method:     args.Op,	 		//put or append
		MethodArgs: methodArgs,
		CmdIndex: args.CmdIndex,				//命令编号
		ClientId: args.ClientId,
	}

	retRes:=Result{
		Err:    ErrWrongLeader,
		RetVal: "",
	}

	if _,isLeader:=kv.rf.GetState(); isLeader{			//只有是leader才能发送命令
		retRes=kv.SendCmdToRaft(args.ClientId,args.CmdIndex,op)			//发送命令
	}else{
		DPrintf("%v(%v) is not the leader,so PutAppend not ok\n",kv.me,kv.gid)
	}

	reply.Err = retRes.Err
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Configuration.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	DPrintf("%v(%v) start kv server\n",kv.me,kv.gid)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(kv.masters)		//构造一个shardmaster集群，为了和其通讯

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.DataBase = make(map[string]string)      //初始化kv数据库
	kv.lastCmdIndexMap = make(map[int64]int) //初始化lastCmdIndex
	kv.clientChannels = make(map[RequestId]chan Result)  //初始化

	kv.LastAppliedIndex = -1
	kv.LastAppliedTerm = -1
	kv.curConfig.Num = 0		//第一个config初始为0

	kv.ReadSnapshot()			//首先从快照中恢复

	go kv.RecApplyMsg() //单独开一个线程用于 读取从raft系统通道里返回的数据

	go kv.FetLasCon()			//fetch 最后的配置


	kv.SetChaFlag(false)
	//kv.isChanging = false

	return kv
}
