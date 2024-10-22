package raft

import (
	"../labgob"
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)


func (rf *Raft) lock(lockName string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = lockName

}

func (rf *Raft) unlock(lockName string) {

	rf.lockEnd = time.Now()

	//if lockName != rf.lockName {
	//	fmt.Printf("unlock (%v) error\n",lockName)
	//	os.Exit(-1)
	//}

	rf.lockName = ""

	rf.mu.Unlock()
}

////log compaction:
//func (rf *Raft) DoSnapShot(lastAppliedIndex , lastAppliedTerm int, snapshot []byte) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	if lastAppliedIndex <= rf.LastIncIndex {
//		return
//	}
//
//	//update last included index & term
//	rf.log = append(make([]Log,0), rf.log[curIdx-rf.lastIncludedIndex:]...)
//	rf.lastIncludedIndex = curIdx
//	rf.lastIncludedTerm = rf.getLog(curIdx).Term
//	rf.persistWithSnapShot(snapshot)
//}

////保存快照和状态,for kvserver
//func (rf*Raft) SaveStateAndSnapshot(snapshot SnapShot){
//
//	//rf.mu.Lock()
//
//	rf.lock("SaveStateAndSnapshot lock")
//	defer rf.unlock("SaveStateAndSnapshot lock")
//
//	//正常情况下，SnapShot的LastAppliedIndex都是大于等于rf.LastIncIndex
//	//但是有些情况从leader那接收到新的snapshot会使得rf.LastIncIndex >= LastAppliedInde
//	if snapshot.LastAppliedIndex <= rf.LastIncIndex{
//		return
//	}
//
//	rf.TrimLogsWithLock(snapshot.LastAppliedIndex)		//裁剪log
//	rf.LastIncIndex = snapshot.LastAppliedIndex
//	rf.LastIncTerm = snapshot.LastAppliedTerm
//
//
//	stats:=Stats{
//		CurrentTerm: rf.CurrentTerm,
//		VoteFor:     rf.VoteFor,
//		Logs:        rf.Logs,
//		LastIncIndex: rf.LastIncIndex,
//		LastIncTerm: rf.LastIncTerm,
//	}
//	snapshotByte,_ := rf.Serialize(snapshot,SnapshotType)		//序列化
//	stateByte,_ := rf.Serialize(stats,StatsType)
//
//	rf.persister.SaveStateAndSnapshot(stateByte,snapshotByte)
//
//	DPrintf("%v save and trim index:term(%v:%v), it's size is %v \n",
//		rf.me,snapshot.LastAppliedIndex,rf.LastIncTerm,rf.GetRaftStateSize())
//
//}

//保存快照和状态,for kvserver
//snapshot 为二进制byte版本
func (rf*Raft) SaveStateAndSnapshotByte(lastAppliedIndex int,lastAppliedTerm int, snapshotByte []byte){

	//rf.mu.Lock()

	rf.lock("SaveStateAndSnapshot lock")
	defer rf.unlock("SaveStateAndSnapshot lock")

	//正常情况下，SnapShot的LastAppliedIndex都是大于等于rf.LastIncIndex
	//但是有些情况从leader那接收到新的snapshot会使得rf.LastIncIndex >= LastAppliedInde
	if lastAppliedIndex <= rf.LastIncIndex{
		return
	}

	rf.TrimLogsWithLock(lastAppliedIndex)		//裁剪log
	rf.LastIncIndex = lastAppliedIndex
	rf.LastIncTerm = lastAppliedTerm


	stats:=Stats{
		CurrentTerm: rf.CurrentTerm,
		VoteFor:     rf.VoteFor,
		Logs:        rf.Logs,
		LastIncIndex: rf.LastIncIndex,
		LastIncTerm: rf.LastIncTerm,
	}
	//snapshotByte,_ := rf.Serialize(snapshot,SnapshotType)		//序列化
	stateByte,_ := rf.Serialize(stats,StatsType)

	rf.persister.SaveStateAndSnapshot(stateByte,snapshotByte)

	DPrintf("%v save and trim index:term(%v:%v), it's size is %v \n",
		rf.me,lastAppliedIndex,rf.LastIncTerm,rf.GetRaftStateSize())

}

//保存快照和状态,for kvserver
//snapshot 为二进制byte版本
func (rf*Raft) SaveStateAndSnapshotByteWithLock(lastAppliedIndex int,lastAppliedTerm int, snapshotByte []byte){

	//rf.mu.Lock()

	//rf.lock("SaveStateAndSnapshot lock")
	//defer rf.unlock("SaveStateAndSnapshot lock")

	//正常情况下，SnapShot的LastAppliedIndex都是大于等于rf.LastIncIndex
	//但是有些情况从leader那接收到新的snapshot会使得rf.LastIncIndex >= LastAppliedInde
	if lastAppliedIndex <= rf.LastIncIndex{
		return
	}

	rf.TrimLogsWithLock(lastAppliedIndex)		//裁剪log
	rf.LastIncIndex = lastAppliedIndex
	rf.LastIncTerm = lastAppliedTerm


	stats:=Stats{
		CurrentTerm: rf.CurrentTerm,
		VoteFor:     rf.VoteFor,
		Logs:        rf.Logs,
		LastIncIndex: rf.LastIncIndex,
		LastIncTerm: rf.LastIncTerm,
	}
	//snapshotByte,_ := rf.Serialize(snapshot,SnapshotType)		//序列化
	stateByte,_ := rf.Serialize(stats,StatsType)

	rf.persister.SaveStateAndSnapshot(stateByte,snapshotByte)

	DPrintf("%v save and trim index:term(%v:%v), it's size is %v \n",
		rf.me,lastAppliedIndex,rf.LastIncTerm,rf.GetRaftStateSize())

}

//
////for requestSnapshot
//func (rf*Raft) SaveStateAndSnapshotWithLock(snapshot SnapShot){
//
//
//	rf.LastIncIndex = snapshot.LastAppliedIndex
//	rf.LastIncTerm = snapshot.LastAppliedTerm
//
//	//rf.TrimLogsWithLock(trimIndex)		//裁剪log
//
//	stats:=Stats{
//		CurrentTerm: rf.CurrentTerm,
//		VoteFor:     rf.VoteFor,
//		Logs:        rf.Logs,
//		LastIncIndex: rf.LastIncIndex,
//		LastIncTerm: rf.LastIncTerm,
//	}
//	snapshotByte,_ := rf.Serialize(snapshot,SnapshotType)		//序列化
//	stateByte,_ := rf.Serialize(stats,StatsType)
//
//	rf.persister.SaveStateAndSnapshot(stateByte,snapshotByte)
//
//	DPrintf("%v save and trim index:term(%v:%v), it's size is %v \n",
//		rf.me,snapshot.LastAppliedIndex,rf.LastIncTerm,rf.GetRaftStateSize())
//
//}


func (rf* Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
}




func (rf* Raft) GetLastLogEntryWithLock() LogEntry {

	if len(rf.Logs) == 0{
		panic("logs len equal 0 in GetLastLogEntryWithLock")			///???
	}else{
		return rf.Logs[len(rf.Logs) - 1]
	}
}

func (rf* Raft) GetLastLogEntry() LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.Logs) == 0{
		panic("logs len equal 0 in GetLastLogEntryWithLock")			///???
	}else{
		return rf.Logs[len(rf.Logs) - 1]
	}
}

func (rf*Raft) GetActIndexWithLock(logIndex int)int{

	actIndex:=-1
	if  len(rf.Logs)>0{
		firstLogIndex:=rf.Logs[0].Index
		lastLogIndex:= rf.Logs[len(rf.Logs)-1].Index

		//fmt.Printf("%v:logIndex(%v) vs firIndex(%v)~lastIndex(%v)\n",rf.me,logIndex,firstLogIndex,lastLogIndex)

		//在范围log index之内
		if logIndex>= firstLogIndex && logIndex<=lastLogIndex{
			actIndex=logIndex-firstLogIndex
		}
	}

	return actIndex
}

func (rf* Raft) GetLogAtIndexWithLock(logIndex int)(LogEntry,bool){

	isOk:=false
	log:=LogEntry{}

	if actIndex:=rf.GetActIndexWithLock(logIndex);actIndex != -1{
		log=rf.Logs[actIndex]
		isOk = true
	}

	return log,isOk
}

func (rf* Raft) TrimLogsWithLock(logIndex int) {

	//fmt.Printf("test: logIndex(%v) vs logLen(%v)\n",logIndex,len(rf.Logs))
	idx:=rf.GetActIndexWithLock(logIndex)

	if idx != -1{
		rf.Logs = rf.Logs[idx:]		//保留logIndex日志
	}else{
		fmt.Printf("TrimLogsWithLock error ")	//TODO: 暂时放在这儿
		os.Exit(-1)
	}
}



//序列化
func (rf* Raft) Serialize(srcData interface{},serType string) ([]byte,bool) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var retByte []byte
	isOk := false

	switch serType {
	case SnapshotType:
		snapshot:=srcData.(SnapShot)
		e.Encode(snapshot)			//序列化Snapshot类型
		isOk = true
		break
	case StatsType:
		stats:=srcData.(Stats)			//强制转化为Stats类型
		e.Encode(stats)				//序列化Stats类型
		isOk = true
		break

	}
	if isOk{
		retByte = w.Bytes()
	}

	return retByte,isOk

}

//反序列化
func (rf* Raft) Deserialize(byteData []byte,serType string) (interface{},bool) {

	if serType == SnapshotType{
		log.Fatalf("should not deserialize SnapshoType\n")
		return nil, false
	}

	//如果传入的byte数据为空的话，直接退出
	if byteData == nil{
		return nil,false
	}

	r := bytes.NewBuffer(byteData)
	d := labgob.NewDecoder(r)

	isOk := false
	var retInt interface{}

	switch serType {
	case SnapshotType:
		var snapshot SnapShot
		d.Decode(&snapshot)
		retInt = snapshot
		isOk = true
		break
	case StatsType:
		var stats Stats
		d.Decode(&stats)
		retInt = stats
		isOk = true
		break
	}

	return retInt,isOk

}




//互斥返回结点的role
func (rf* Raft) Role() int {
	var role int
	rf.mu.Lock()
	role = rf.role
	rf.mu.Unlock()
	return role
}



//更新周期，用于request和response中
func (rf* Raft) UpdateTerm(term int) bool{

	isUpdated:= false

	//rf.mu.Lock()
	rf.lock("UpdateTerm lock")

	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.InitFollowerWithLock(-1)				//不投票给任何server
		isUpdated = true
	}

	//rf.mu.Unlock()
	rf.unlock("UpdateTerm lock")

	return isUpdated		//返回是否进行了term更新，也反映了当前node是否被强制成为了FOLLOWER
}


//测试使用
func (rf* Raft) ShowLog(){

	for _,log:= range  rf.Logs{
		DPrintf("node(%v) has log:%v(term) %v(index) %v(cmd)",rf.me,log.Term,log.Index,log.Cmd)
	}
}


//供kvserver调用
func (rf* Raft) GetRaftStateSize()int{

	//rf.mu.Lock()
	size:=rf.persister.RaftStateSize()
	//rf.mu.Unlock()

	return size
}

//判断candidate的log信息是否更新
//进入此函数时已经申请到锁
func (rf *Raft)UpToDate(args *RequestVoteArgs) bool{
	isNew:=false

	tmpLastLog:=rf.GetLastLogEntryWithLock()

	if args.LastLogTerm < tmpLastLog.Term {
		isNew = false
	}else if args.LastLogTerm > tmpLastLog.Term {
		isNew = true
	}else if args.LastLogTerm == tmpLastLog.Term {
		if args.LastLogIndex >= tmpLastLog.Index {
			isNew = true
		}else{
			return false
		}
	}

	return isNew
}

