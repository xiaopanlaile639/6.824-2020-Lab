package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	mu      sync.Mutex
	id	 int64
	cmdIndex	int64
	lastLeader	int

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.id = time.Now().UnixNano()			//构造id
	ck.cmdIndex = 0			//从0条命令开始
	ck.lastLeader = 0


	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	retStr := ""

	newCmdIndex:=ck.IncCmdIndex()		//增加命令编号

	args:= GetArgs{
		Key: key,
		ClientId: ck.id,
		CmdIndex: newCmdIndex,
	}
	reply:=GetReply{}

	//循环发送请求，知道返回成功或失败
	for {

		DPrintf("client(%v） Get key(%v) from %v...\n",ck.id,key,ck.lastLeader)
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)

		if ok{
			switch reply.Err {
			case OK:
				retStr = reply.Value
				DPrintf("client(%v） Get key(%v)->val（%v） from %v OK.\n",ck.id,key,reply.Value,ck.lastLeader)
				return retStr
			case ErrNoKey:
				retStr = ""
				DPrintf("%v Get to server %v failed(%v) change next...\n",ck.id,ck.lastLeader,reply.Err)
				return retStr
			case ErrWrongLeader:
				DPrintf("%v Get to server %v failed(%v) change next...\n",ck.id,ck.lastLeader,reply.Err)
				time.Sleep(ChangeLeaderInterval)
				ck.NextLeader()
				continue
			case TimeOut:
				DPrintf("%v Get to server %v failed(%v)  retry...\n",ck.id,ck.lastLeader,reply.Err)
				//ck.NextLeader()
				continue
			}
		}else{
			DPrintf("%v Call KVServer.Get error",ck.lastLeader)
		}


		//if o	k && reply.Err == OK { //如果返回成功
		//
		//	DPrintf("client(%v） Get key(%v)->val（%v） from %v OK.\n",ck.id,key,reply.Value,ck.lastLeader)
		//	break
		//}else{
		//	DPrintf("%v Get to server %v failed(%v) change next...\n",ck.id,ck.lastLeader,reply.Err)
		//	ck.NextLeader()
		//}

	}

	//return retStr
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	newCmdIndex:=ck.IncCmdIndex()


	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientId: ck.id,
		CmdIndex: newCmdIndex,
	}

	reply:=PutAppendReply{ }

	//循环发送请求，知道返回成功或失败
	for {

		DPrintf("(%v）Client %v {%v->%v} to %v...\n",ck.id,op,key,value,ck.lastLeader)
		ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK{			//如果成功提交，返回成功
			DPrintf("client %v {%v->%v} OK",op,key,value)
			break
		}else{
			DPrintf("%v PutAppend to server %v failed(%v) change next...\n",ck.id,ck.lastLeader,reply.Err)
			ck.NextLeader()				//更换下一个kv server，重新发送请求
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//互斥增加命令编号
func (ck*Clerk)IncCmdIndex()int64{

	ck.mu.Lock()
	ck.cmdIndex++
	retCmdIndex:=ck.cmdIndex
	ck.mu.Unlock()

	return retCmdIndex
}

func (ck*Clerk)NextLeader(){
	ck.mu.Lock()
	ck.lastLeader = (ck.lastLeader+1) % len(ck.servers)
	ck.mu.Unlock()
}

