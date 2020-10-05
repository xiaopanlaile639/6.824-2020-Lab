package raft

import (
	"fmt"
	//"bytes"
	//"kvraft"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
 //import "bytes"
 //import "../labgob"

//获取结点是否是leader状态
func (rf *Raft) GetState() (int, bool) {

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.lock("GetState lock")
	defer rf.unlock("GetState lock")

	term := rf.CurrentTerm
	isLeader:=false
	if rf.role == LEADER{
		isLeader = true
	}
	return term, isLeader
}

//持久化结点状态
func (rf *Raft) persist() {

	stats:=Stats{
		CurrentTerm:  rf.CurrentTerm,
		VoteFor:      rf.VoteFor,
		Logs:         rf.Logs,
		LastIncIndex: rf.LastIncIndex,
		LastIncTerm:  rf.LastIncTerm,
	}

	statsByte,isOk:= rf.Serialize(stats,StatsType)
	if isOk{
		rf.persister.SaveRaftState(statsByte)
	}else{
		DPrintf("Serialize error\n")
	}

}

//读取上一个持久化的状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	statsInt,isOk:=rf.Deserialize(data,StatsType)

	if isOk{
		stats := statsInt.(Stats)

		rf.CurrentTerm = stats.CurrentTerm
		rf.VoteFor = stats.VoteFor
		rf.Logs = stats.Logs
		rf.LastIncIndex = stats.LastIncIndex
		rf.LastIncTerm = stats.LastIncTerm
	}else{
		DPrintf("Decode stats error\n")
	}

}

//////////////////////////以下是RPC相关处理//////////////////////////////


//处理leader发来的log请求（也有可能是heartbeat消息）
func (rf *Raft) AppendEntries(args * AppendEntriesArgs, reply *AppendEntriesReply) {

	isUpdated := rf.UpdateTerm(args.Term) //更新term

	isUpdateCommit := false
	reply.Success = false

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.lock("AppendEntries lock")
	defer rf.unlock("AppendEntries lock")

	//如果两个term相等，说明有可能在前面的UpdateTerm中进行了修改，即
	//leader的term大于或等于rf.CurrentTerm,就说明这个rf承认了leader的地位
	if args.Term == rf.CurrentTerm{

		if isUpdated == false {		//如果未被更新，说明原本就是同一周期
			rf.InitFollowerWithLock(rf.VoteFor)		//已经投票
		}

		tmpPrevLogIndex := args.PrevLogIndex		//leader的上一个日志index
		tmpTerm:=args.PreLogTerm							//leader的上一个日志term

		//DPrintf("%v's log range:(%v-%v), args's PreLogIndex:%v\n",
		//	rf.me,rf.Logs[0].Index,rf.GetLastLogEntryWithLock().Index,tmpPrevLogIndex)

		preLog,isOk:=rf.GetLogAtIndexWithLock(tmpPrevLogIndex)


		//匹配
		logOk := false
		if isOk && preLog.Term == tmpTerm{
			logOk = true
		}

		if  logOk{ //index和term都符合

			reply.Success = true
			isConfilct:=false

			localLogIndex :=tmpPrevLogIndex+1
			leaderLogIndex :=0
			actIndex:=rf.GetActIndexWithLock(localLogIndex)		//实际在log中的开始索引处

			//遍历leader传入的参数
			for ; leaderLogIndex <len(args.Entries); leaderLogIndex++{
				if  actIndex < len(rf.Logs) && actIndex>=0{ //在小于本地日志长度范围内比较
					if rf.Logs[actIndex].Term != args.Entries[leaderLogIndex].Term{
						isConfilct = true
						break				//发生冲突退出
					}
				}else {					//超过了范围
					break
				}
				actIndex++ //本地日志index索引向后推移
			}

			if isConfilct{				//如果有冲突
				rf.Logs = rf.Logs[:actIndex] //删除所有冲突的元素

			}
			//复制剩下的元素
			rf.Logs = append(rf.Logs,args.Entries[leaderLogIndex:]...)

			//更新CommitIndex
			if args.LeaderCommit > rf.CommitIndex {
				isUpdateCommit = true
				tmpLastEntryIndex:= rf.GetLastLogEntryWithLock().Index

				if tmpLastEntryIndex < args.LeaderCommit{
					rf.CommitIndex = tmpLastEntryIndex
				}else {
					rf.CommitIndex = args.LeaderCommit
				}
			}

			rf.persist()			//持久化存储

		}else{			//log and term 不匹配,或者说rf中不含有preLog

			if isOk == false{				//node结点的日志小于preLogIndex

				reply.ConfilctTerm = -1		//没有冲突的term
				reply.ConflictIndex = rf.GetLastLogEntryWithLock().Index+1   //期望leader结点下一次发送的log index

				DPrintf("%v can't find %v log\n",rf.me,tmpPrevLogIndex)

			}else{				//term不匹配

				reply.ConfilctTerm = preLog.Term
				//寻找第一个term的冲突index
				for _,entry:= range rf.Logs{
					if entry.Term == reply.ConfilctTerm{
						reply.ConflictIndex = entry.Index
						break
					}
				}
				DPrintf("leader's(%v) last log's index:term(%v:%v) not match with %v's (%v:%v)\n",
					args.LeaderId,args.PrevLogIndex, args.PreLogTerm,
					rf.me,preLog.Index,preLog.Term)
			}

		}
	}

	reply.Term = rf.CurrentTerm		//失败、成功返回的Term形式上是一样的

	//异步发送提交消息
	if isUpdateCommit{
		rf.Applycond.Signal()
	}

}


//发送log消息（也有可能是heartbeat消息）给followers
func (rf *Raft) SendAppendEntries(server int, args * AppendEntriesArgs, reply *AppendEntriesReply)bool {

	if len(args.Entries) != 0 {
		DPrintf("%v(role:%v) send MsgIndex(%v) to %v",rf.me,rf.role,args.PrevLogIndex+1,server)
	}else {
		DPrintf("%v(role:%v) send HeartBeat to %v",rf.me,rf.role,server)
	}

	ok:= rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

//请求投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.UpdateTerm(args.Term)			//不管三七二十一，只要是对方的term较高，就采用对方的

	//rf.mu.Lock()			//加锁
	//defer rf.mu.Unlock()

	rf.lock("RequestVote lock")
	defer rf.unlock("RequestVote lock")

	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm

	if args.Term<rf.CurrentTerm {		//如果candidate的Term较小
		reply.VoteGranted = false
	}else if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		if rf.UpToDate(args){			//如果更新
			reply.VoteGranted = true				//投票
			rf.InitFollowerWithLock(args.CandidateId)     //转化为Followers,并投票给CandidateId
		}else{
			reply.VoteGranted = false
		}
	}else{
		reply.VoteGranted = false
	}

	if reply.VoteGranted{
		DPrintf("%v vote to %v",rf.me,args.CandidateId)
	}else{
		DPrintf("%v refuse to vote to %v",rf.me,args.CandidateId)
	}

}

//请求Snapshot
func  (rf *Raft) RequestSnapshot(args *InstallSnapArgs, reply *InstallSnapReplys) {

	rf.UpdateTerm(args.Term)

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.lock("RequestSnapshot lock")
	defer rf.unlock("RequestSnapshot lock")

	curTerm:=rf.CurrentTerm

	reply.Term = curTerm
	reply.Ok = false

	if args.Term == curTerm {		//如果发生了更新或者term一样大

		//rf.mu.Lock()
		if rf.LastIncIndex >= args.LastIncludedIndex{		//节点本身已经保存了更高index的快照
			//rf.mu.Unlock()
			return
		}

		//TODO: 修改：裁剪日志和持久保存可以放在一起
		_,isOk:=rf.GetLogAtIndexWithLock(args.LastIncludedIndex)		///???

		if isOk{				//找到
			rf.TrimLogsWithLock(args.LastIncludedIndex)
		}else {		//如果没找到的话，清空log，并且需要再日志中重新建一个虚拟的log
			//fmt.Printf("GetLogAtIndexWithLock error in RequestSnapshot")
			//os.Exit(-1)
			rf.Logs = make([]LogEntry, 1)
			rf.Logs[0].Index = args.LastIncludedIndex
			rf.Logs[0].Term = args.LastIncludedTerm
			//rf.Logs[0]

		}

		//更新commitIndex
		rf.CommitIndex = args.LastIncludedIndex
		rf.LastIncTerm = args.LastIncludedTerm

		rf.SaveStateAndSnapshotWithLock(args.Snapshot)			//保存leader发来的快照

		rf.Applycond.Signal()			//释放信号

		reply.Ok = true
		reply.Term=curTerm
		//rf.mu.Unlock()

	}


}

//需要我们来保证sendRequestVote的时候是在另一个goruntine
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//leader 发送Snapshot rpc
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapArgs, reply *InstallSnapReplys)bool {
	ok := rf.peers[server].Call("Raft.RequestSnapshot", args, reply)
	return ok
}

//////////////////////////以上是RPC相关处理///////////////////////////////////////////////////


//执行一条上层发来的命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := false

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.lock("Start lock")
	defer rf.unlock("Start lock")

	if rf.role == LEADER {			//只有Leader才接受请求

		//返回的信息
		index = rf.GetLastLogEntryWithLock().Index+1 		//此log的index
		term = rf.CurrentTerm
		isLeader = true

		DPrintf("%v received cmd:index(%v:%v)  from client\n",rf.me,command,index)

		//构造日志
		log := LogEntry{
			Term:  rf.CurrentTerm,
			Index: index,
			Cmd:   command,
		}

		//加入自己的log日志中
		rf.Logs = append(rf.Logs,log) //添加一条log信息

		rf.persist()

		rf.NextIndex[rf.me] = rf.GetLastLogEntryWithLock().Index+1             //更新下一个log的index
		rf.MatchIndex[rf.me]=rf.NextIndex[rf.me]-1			//更新leader的已复制log标志

		rf.BroadcastLog(false) //向其他的follow广播log消息
	}

	return index, term, isLeader
}


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//运行Raft实例，
//Raft三个状态切换都在这一个goroutine中
//note:同步语句还未修改
func (rf *Raft) RaftRunning(){

	for{
		//rf.mu.Lock()			//例程会花费大量的时间耗在这儿
		//role:= rf.role
		//rf.mu.Unlock()

		rf.lock("RaftRunning lock")
		role:= rf.role
		rf.unlock("RaftRunning lock")

		switch role {
			case FOLLOWER:			//Follower的等待事件
				rf.WaitToBecCan()
			case CANDIDATE:
				rf.StartElection()
			case LEADER:

				rf.BroadcastLog(true) //周期性发送心跳消息

				if rf.Role() == LEADER {			//如果角色还是LEADER的话
					time.Sleep(time.Millisecond*time.Duration(150))		//休眠
				}

		}

	}
}


//提交log线程
func (rf* Raft) doCommit() {

	for{

		rf.lock("doCommit lock")

		//如果CommitIndex<=LastApplied的话，就循环等待???(为啥要循环等待呢)？？？
		for rf.CommitIndex <= rf.LastApplied {
			rf.Applycond.Wait()
		}

		var msgs []ApplyMsg

		if rf.LastApplied < rf.LastIncIndex {
			tmpMsg:=ApplyMsg{
				CommandValid: false,
				CommandIndex: rf.LastIncIndex,
				CommandTerm: rf.LastIncTerm,
			}
			msgs = append(msgs,tmpMsg)

		}else if rf.LastApplied < rf.CommitIndex{

			for i:=rf.LastApplied+1;i<=rf.CommitIndex;i++{

				tmpLog,isOk:=rf.GetLogAtIndexWithLock(i)
				if isOk && tmpLog.Cmd != nil{
					tmpMsg:=ApplyMsg{
						CommandValid: true,
						Command:      tmpLog.Cmd,
						CommandIndex: i,
						CommandTerm:  tmpLog.Term,
					}

					msgs = append(msgs,tmpMsg)
				}else {

					//fmt.Printf("GetLogAtIndexWithLock error in doCommit")
					//os.Exit(-1)
					DPrintf("%v can't find cmd(%v)\n",rf.me,i)
				}
			}
		}

		rf.unlock("doCommit lock")

		//将commit 的log信息发送给kv server
		for _,msg:=range msgs{

			rf.applyCh<-msg

			rf.lock("applyLog lock in doCommit")
			DPrintf("%v apply cmd:index:term(%v:%v:%v) ---CommitIndex(%v) \n",rf.me,msg.Command,msg.CommandIndex,msg.CommandTerm,rf.CommitIndex)
			rf.LastApplied = msg.CommandIndex
			rf.unlock("applyLog lock in doCommit")
		}

	}
}



////提交log线程
//func (rf* Raft) doCommit() {
//
//	for{
//
//		//rf.mu.Lock()
//
//		rf.lock("doCommit lock")
//
//		//如果CommitIndex<=LastApplied的话，就循环等待???(为啥要循环等待呢)？？？
//		for rf.CommitIndex <= rf.LastApplied {
//			rf.Applycond.Wait()
//		}
//
//		if rf.LastApplied < rf.LastIncIndex {
//
//			appMsg:=ApplyMsg{
//				CommandValid: false,
//			}
//
//			rf.LastApplied = rf.LastIncIndex
//
//			//rf.mu.Unlock()
//
//			rf.unlock("doCommit lock")
//
//			rf.applyCh<-appMsg
//
//			rf.lock("doCommit lock")
//
//			//rf.mu.Lock()
//			//continue
//		}
//
//
//		i:=rf.LastApplied+1
//		tmpCommitIndex:=rf.CommitIndex
//
//		for ; i<=tmpCommitIndex;i++ {
//
//			tmpLog,isOk:=rf.GetLogAtIndexWithLock(i)
//			if isOk && tmpLog.Cmd != nil{			//为了防止tmpLog 已经在保存快照的时候删除
//				DPrintf("%v apply Index %v(%v)  \n",rf.me,i,tmpLog.Index)
//
//				msg:=ApplyMsg{
//					CommandValid: true,
//					Command:      tmpLog.Cmd,
//					CommandIndex: i,
//					CommandTerm: tmpLog.Term,
//				}
//
//				//rf.mu.Unlock()
//				rf.unlock("doCommit lock")
//				rf.applyCh<-msg
//				//rf.mu.Lock()
//				rf.lock("doCommit lock")
//
//				rf.LastApplied = i
//
//			}
//
//		}
//
//		rf.unlock("doCommit lock")
//		//rf.mu.Unlock()
//
//	}
//}

//创造一个Raft实例
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me


	rf.Applycond = sync.NewCond(&rf.mu)		//给条件变量绑定锁
	rf.applyCh = applyCh			//将参数通道复制给rf中
	rf.VoteFor = -1					//并没参加选举
	rf.voteNum=0					//初始化投票的数量

	rf.CurrentTerm = 0				//初始化当前term
	rf.CommitIndex = 0				//初始化CommitIndex
	rf.LastApplied = 0				//初始化lastApplied

	rf.Logs = make([]LogEntry,1)		//idx=0, 存放lastSnapshot
	rf.LastIncIndex = 0
	rf.LastIncTerm = 0


	rand.Seed(time.Now().UnixNano())		//设置随机数种子

	DPrintf("raft %v init...\n",me)
	rf.role = FOLLOWER
	rf.elapsTime = 0
	rf.waitTime = rand.Intn(RANDTIME)+FOL_BASE_TIME	//设置下次超时的时间

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go rf.RaftRunning()			//实例运行
	go rf.doCommit()			//新开一个提交的单独例程


	return rf
}

func (rf* Raft)LockDebug(){
	for !rf.killed(){
		time.Sleep(time.Second*2)

		fmt.Printf("rf who(%v) has lock(%v) ,time(%v)\n",rf.me,rf.lockName,time.Now().Sub(rf.lockStart))
	}
}