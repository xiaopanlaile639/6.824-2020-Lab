package raft

import (
	"fmt"
	"os"
)

//初始化成为LEADER
//进入此函数时已经拿到锁
func (rf* Raft) InitLeaderWithLock(){
	rf.role = LEADER

	//初始化nextIndex和MatchIndex
	serverNum:=len(rf.peers)
	rf.NextIndex = make([]int,serverNum)
	rf.MatchIndex = make([]int, serverNum)

	//rf.nextIndex初始化为 leader的last Logs index+1
	for i:=0;i<len(rf.peers);i++ {
		rf.NextIndex[i] =  rf.GetLastLogEntryWithLock().Index+1
		rf.MatchIndex[i] = 0	//rf.Match 初始化为0
	}


	rf.persist()				//持久化存储,这个地方不知道是否必要？？？

	DPrintf("%v become the leader",rf.me)


}



//广播Log信息
//默认先广播一条log,log不一定是最新的，而是nextIndex所指示的那条log
//进入此函数时，还未获得锁
func (rf *Raft) BroadcastLog( isHeartMsg bool){

	//向所有的结点发送消息
	for i,_ := range rf.peers {
		if i!= rf.me {

			go rf.SendLogOrSnapshot(i,isHeartMsg)		//发送快照或者普通log
			//go rf.RequestAppendNewEntries(i,isHeartMsg)
		}
	}
}


//return true if update success
//更新leader的CommitIndex
//note:未采用参考答案的做法
func (rf* Raft) UpdateCommitForLeader(tmpCurIndex int,tmpCurTerm int) bool {

	updated := false

	if tmpCurIndex > rf.CommitIndex{		//node的已复制logIndex大于leader的CommitIndex
		tmpMatchNum:=0
		//统计已经复制tmpIndex这个log的结点数量
		for i,_:= range rf.peers {
			if rf.MatchIndex[i] >= tmpCurIndex{
				tmpMatchNum++
			}
		}

		//如果复制的数量大于一半并且其term正好是leader的当前的term
		if tmpMatchNum > len(rf.peers)/2 && tmpCurTerm == rf.CurrentTerm {			//大于一半的人已经复制了log

			rf.CommitIndex = tmpCurIndex
			updated = true		//更新标志

		}
	}

	return updated			//返回是否更新了leader的commitIndex
}

//处理appendEntries的回复消息
func (rf* Raft) HandleAppendEntriesResponse(args AppendEntriesArgs,reply AppendEntriesReply,server int) bool {

	rf.UpdateTerm(reply.Term) //只要有response或者request都需要检测更新一下Term

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.lock("HandleAppendEntriesResponse lock")
	defer rf.unlock("HandleAppendEntriesResponse lock")

	isContinue := false
	isUpdated := false

	if reply.Success { //follower成功接受消息

		rf.MatchIndex[server] = len(args.Entries) + args.PrevLogIndex	//为了防止收到重复的success消息，增加的log数目应该使用这种方式
		rf.NextIndex[server] = rf.MatchIndex[server] + 1

		isUpdated = rf.UpdateCommitForLeader(rf.MatchIndex[server], args.Term)		//参数是当前server的matchIndex

	}else {				//如果发送日志失败，nextIndex需要减1，并重新尝试

		//快速回退
		var i int
		for i = len(rf.Logs)-1;i>=0;i--{				//从后往前搜索
			if rf.Logs[i].Term == reply.ConfilctTerm {
				rf.NextIndex[server] = rf.Logs[i].Index  + 1 //下一个term不等于confilctTerm的index
				break
			}
		}

		if i == -1{					//如果在leader的term中没找到冲突的term
			rf.NextIndex[server] = reply.ConflictIndex
		}

		isContinue = true
	}
	if isUpdated {
		rf.Applycond.Signal()			//通知commit更新信息
	}

	return isContinue		//返回是否retry 发送log
}

//向peerIndex发送log同步请求
//进入此函数时处于加锁状态
func (rf* Raft) RequestAppendNewEntries(peerIndex int,isHeartBeat bool) {

	var entries []LogEntry
	//rf.mu.Lock()

//	rf.lock("RequestAppendNewEntries lock")

	if rf.role == LEADER {

		//构造peerIndex结点的entries作为参数
		for i:= rf.NextIndex[peerIndex];i<=rf.GetLastLogEntryWithLock().Index;i++{
			entry,isOk:=rf.GetLogAtIndexWithLock(i)

			if isOk == false{
				fmt.Printf("%v's nextIndex for %v is %v\n",rf.me,peerIndex,rf.NextIndex[peerIndex])
				fmt.Printf("%v GetLogAtIndexWithLock for %v error in RequestAppendNewEntries 1 (" +
					"want log index %v range(%v:%v))\n",rf.me,peerIndex,i,rf.Logs[0].Index,rf.GetLastLogEntryWithLock().Index)
				os.Exit(-1)
			}
			entries = append(entries,entry)
		}

		//下面这条语句保证了，如果日志中还有未发送的log，则心跳消息也当做log消息发送
		if len(entries) == 0 && isHeartBeat == false{			//如果不是心跳消息，或者需要发送的log为0，则直接退出
		//	rf.unlock("RequestAppendNewEntries lock")
			//rf.mu.Unlock()
			return
		}

		tmpPreIndex:= rf.NextIndex[peerIndex]-1
		tmpPreLog, _isOk :=rf.GetLogAtIndexWithLock(tmpPreIndex)

		if _isOk == false{
			fmt.Printf("%v's tmpPreIndex for %v is %v\n",rf.me,peerIndex,tmpPreIndex)
			fmt.Printf("%v GetLogAtIndexWithLock for %v error in RequestAppendNewEntries 2 (" +
				"want log index %v range(%v:%v))\n",rf.me,peerIndex,tmpPreIndex,rf.Logs[0].Index,rf.GetLastLogEntryWithLock().Index)

			os.Exit(-1)
		}

		//初始化AppendEntries参数
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex,
			Entries: entries,			//有可能是心跳消息
			PrevLogIndex: tmpPreIndex,		//上一条日志
			PreLogTerm: tmpPreLog.Term,		//上一条日志的Term
		}

		reply := AppendEntriesReply{}

		rf.unlock("RequestAppendNewEntries lock")

		ok:= rf.SendAppendEntries(peerIndex,&args,&reply)

		if ok {
			isContinue := rf.HandleAppendEntriesResponse(args,reply,peerIndex)

			if isContinue {			//需要继续发送
				//go rf.RequestAppendNewEntries(peerIndex,false)

				//发送日志或者快照
				go rf.SendLogOrSnapshot(peerIndex,false)
			}
		} else {			//未正常返回replay，有可能是因为等待超时
			DPrintf("%v receive no reply(Logs) from %v",rf.me,peerIndex)
		}

		rf.lock("RequestAppendNewEntries")			//进入加锁，退出也要处于加锁状态
	}
}

//给server发送snapshot
//note:进入此函数时已上锁
func (rf *Raft) RequestInstallSnapshot(server int ){

	//rf.mu.Lock()
	//rf.lock("RequestInstallSnapshot lock")
	if rf.role == LEADER{

		//if rf.isInstallingSnapshot[server] == 0 {
		//	rf.isInstallingSnapshot[server] = 1
		//} else {
		//
		////	rf.mu.Unlock()
		//	//rf.unlock("RequestInstallSnapshot lock")
		//	return
		//}


		lastSnapshotByte:=rf.ReadSnapshot()

		deInt,isOK:=rf.Deserialize(lastSnapshotByte,SnapshotType)	//反序列化

		if isOK {

			snapshot := deInt.(SnapShot) //强制转化为Snapshot类型

			tmpLastIncIndex := rf.LastIncIndex
			tmpLastIncTerm := rf.LastIncTerm

			args := InstallSnapArgs{
				Term:              rf.CurrentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: tmpLastIncIndex, //？？？需不需要加上这两个参数？？？
				LastIncludedTerm:  tmpLastIncTerm,  //???
				Snapshot:          snapshot,
				Done:              false,
			}

			reply := InstallSnapReplys{}

			rf.unlock("RequestInstallSnapshot lock")

			ok:=rf.sendInstallSnapshot(server, &args, &reply)			//leader通过rpc发送snapshot

			if ok{
				rf.UpdateTerm(reply.Term)		//更新操作

				if reply.Ok == false{			//提交失败
					DPrintf("%v send snapshot to %v failed\n",rf.me,server)
				}else {					//snapshot安装成功

					rf.lock("RequestInstallSnapshot lock")

					//更新nextIndex和matchIndex
					rf.NextIndex[server] = args.LastIncludedIndex+1
					rf.MatchIndex[server] = args.LastIncludedIndex

					//rf.isInstallingSnapshot[server] = 0		//???

					rf.unlock("RequestInstallSnapshot lock")

					DPrintf("%v send snapshot to %v ok\n",rf.me,server)
				}

			}else{
				////释放
				//rf.lock("RequestInstallSnapshot lock")
				//rf.isInstallingSnapshot[server] = 0
				//rf.unlock("RequestInstallSnapshot lock")
			}

			rf.lock("RequestInstallSnapshot lock")		//进入加锁，退出函数时也需要加锁
		}
	}

}

func (rf *Raft) SendLogOrSnapshot(server int,isHeartBeat bool){

	//rf.mu.Lock()
	rf.lock("SendLogOrSnapshot lock")
	defer  rf.unlock("SendLogOrSnapshot lock")

	if rf.role == LEADER {

		nextIndex:=rf.NextIndex[server]
		lastIncIndex:=rf.LastIncIndex

		if nextIndex <= lastIncIndex {		//小于快照位置
			rf.RequestInstallSnapshot(server)
		}else{
			rf.RequestAppendNewEntries(server,isHeartBeat)
		}
	}
}

