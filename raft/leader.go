package raft

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
		rf.NextIndex[i] = len(rf.Logs) //？？？这里还有些疑问，到底需不需要-1
	}

	//DPrintf("leader(%v)'s long length:%v\n",rf.me,len(rf.Logs))

	//rf.Match 初始化为0
	for i:=0;i<len(rf.peers);i++{
		rf.MatchIndex[i] = 0
	}

	rf.persist()				//持久化存储,这个地方不知道是否必要？？？

	DPrintf("%v become the leader",rf.me)
	//rf.ShowLog()

}




//广播Log信息
//默认先广播一条log,log不一定是最新的，而是nextIndex所指示的那条log
//进入此函数时，还未获得锁
func (rf *Raft) BroadcastLog( isHeartMsg bool){

	//向所有的结点发送消息
	for i,_ := range rf.peers {
		if i!= rf.me {
			go rf.RequestAppendNewEntries(i,isHeartMsg)
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

	//	DPrintf("test in UPCOMMIT: leader(%v)-tmpCurIndex(%v)-tmpCurTerm(%v)-tmpMatchNum(%v)",rf.me,tmpCurIndex,tmpCurTerm,tmpMatchNum)

		//如果复制的数量大于一半并且其term正好是leader的当前的term
		if tmpMatchNum > len(rf.peers)/2 && tmpCurTerm == rf.CurrentTerm {			//大于一半的人已经复制了log
			//DPrintf("%v's commitIndex is %v, tmpCurIndex is %v\n",rf.me,rf.CommitIndex,tmpCurIndex)

			rf.CommitIndex = tmpCurIndex
			updated = true		//更新标志

			DPrintf("leader's(%v) commitIndex is %v\n",rf.me,rf.CommitIndex)
			//???向follower发送commit通知???
		}
	}

	return updated			//返回是否更新了leader的commitIndex
}

//处理appendEntries的回复消息
func (rf* Raft) HandleAppendEntriesResponse(args AppendEntriesArgs,reply AppendEntriesReply,server int) bool {

	rf.UpdateTerm(reply.Term,server) //只要有response或者request都需要检测更新一下Term


	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.CurrentTerm {	//发送rpc消息需要花费一定时间，可能会阻塞,所以此处需要比较是否已经不再是leader了
		return false
	}
   //如果前后两个term没有发生变化，说明还在同一个周期中
	isContinue := false
	isUpdated := false

	if reply.Success { //follower成功接受消息
		//心跳消息和log消息可以统一处理
		//rf.NextIndex[server] = rf.NextIndex[server] + len(args.Entries) //增加的log数目
		//rf.MatchIndex[server] = rf.NextIndex[server] - 1

		rf.MatchIndex[server] = len(args.Entries) + args.PrevLogIndex	//为了防止收到重复的success消息，增加的log数目应该使用这种方式
		rf.NextIndex[server] = rf.MatchIndex[server] + 1

		isUpdated = rf.UpdateCommitForLeader(rf.MatchIndex[server], args.Term)		//参数是当前server的matchIndex

		//DPrintf("%v's MatchIndex is %v\n",server,rf.MatchIndex[server])

	}else {				//如果发送日志失败，nextIndex需要减1，并重新尝试
		rf.NextIndex[server] = rf.NextIndex[server] - 1
		isContinue = true
	}
	if isUpdated {
		rf.Applycond.Signal()			//通知commit更新信息
	}

	return isContinue		//返回是否retry 发送log
}

//向peerIndex发送log同步请求
func (rf* Raft) RequestAppendNewEntries(peerIndex int,isHeartBeat bool) {

	var entries []LogEntry
	if rf.Role() == LEADER {
		rf.mu.Lock()

		//构造peerIndex结点的entries作为参数
		for i:= rf.NextIndex[peerIndex];i<=rf.GetLastLogEntryWithLock().Index;i++{
			entry := rf.Logs[i]
			entries = append(entries,entry)
		}

		//DPrintf("")

		//下面这条语句保证了，如果日志中还有未发送的log，则心跳消息也当做log消息发送
		if len(entries) == 0 && isHeartBeat == false{			//如果不是心跳消息，或者需要发送的log为0，则直接退出
			rf.mu.Unlock()
			return
		}

		//DPrintf("%v's nextIndex is %v,and leader's log len:%v(or %v)\n",peerIndex,rf.NextIndex[peerIndex],len(rf.Logs),rf.GetLastLogEntryWithLock().Index)


		//初始化AppendEntries参数
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.CommitIndex,
			Entries: entries,			//有可能是心跳消息
			PrevLogIndex: rf.NextIndex[peerIndex]-1,		//上一条日志
			PreLogTerm: rf.Logs[rf.NextIndex[peerIndex]-1].Term,		//上一条日志的Term
		}

		////如果上一条日志Index为0, 如果事先已经插入一个哨兵log，那么这里的判断应该就不需要了
		//if args.PrevLogIndex == 0 {
		//	args.PreLogTerm = rf.CurrentTerm
		//	args.PreLogTerm = rf.CurrentTerm
		//}

		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		ok:= rf.SendAppendEntries(peerIndex,&args,&reply)

		if ok {
			isContinue := rf.HandleAppendEntriesResponse(args,reply,peerIndex)

			if isContinue {			//需要继续发送
				go rf.RequestAppendNewEntries(peerIndex,false)
			}
		} else {			//未正常返回replay，有可能是因为等待超时
			DPrintf("%v receive no reply(Logs) from %v",rf.me,peerIndex)
		}

	}


}