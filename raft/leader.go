package raft

//初始化成为LEADER
//进入此函数时已经拿到锁
func (rf* Raft) InitLeader(){
	rf.role = LEADER

	//初始化nextIndex和MatchIndex
	serverNum:=len(rf.peers)
	rf.NextIndex = make([]int,serverNum)
	rf.MatchIndex = make([]int, serverNum)

	//rf.nextIndex初始化为 leader的last log index+1
	for i,_ := range rf.NextIndex {
		rf.NextIndex[i] = rf.CommitIndex+1
	}

	//rf.Match 初始化为0
	for i,_:=range rf.MatchIndex{
		rf.MatchIndex[i] = 0
	}

	DPrintf("%v become the leader",rf.me)
}


//广播一条日志消息
//进入此函数，还未获得锁
func (rf *Raft) BroadcastLog(server int,args *AppendEntriesArgs,reply *AppendEntriesReply){

	if  rf.SendAppendEntries(server,args,reply) {		//发送消息
		rf.mu.Lock()

		if reply.Success {			//follower成功接受消息

			if args.Entries != nil {		//如果不是心跳消息的回复，即是普通log消息
				//更新leader中保存的node信息
				rf.VotelogNum++

				if rf.VotelogNum >= len(rf.peers)/2 {		//只要赞同log的followers大于一半
					rf.CommitIndex = rf.NextIndex[rf.me]-1	//更新CommitIndex
					//???向follower发送commit通知???
				}
			}
		}else{			//followers未成功log日志,有可能是因为leader的term较小，也有可能是因为PrevLogIndex之前的follower的日志和leader日志有偏差

			//如果是心跳消息的话，应该只能是第一种情况
			if reply.Term > rf.CurrentTerm {			//第一种情况，leader的term较小
				rf.CurrentTerm = reply.Term		//取较大的term
				rf.InitFollower()		//转化为Followers
				DPrintf("leader %v: send LogMsg to %v failed and force to become follower",rf.me,server)
			}else {
				rf.NextIndex[server]--		//对server的log: nextIndex向前移
				DPrintf("leader %v: send LogMsg to %v failed and it's nextLogIndex move forward 1 step ",rf.me,server)

			}

		}

		rf.mu.Unlock()
	} else {			//未正常返回replay，有可能是因为等待超时
		DPrintf("%v:receive no reply(log) from %v",rf.me,server)
	}

}

//广播Log信息
//默认先广播一条log,log不一定是最新的，而是nextIndex所指示的那条log
//进入此函数时，还未获得锁
//???可能有一个问题：被动更新follower的日志
func (rf *Raft) InitBroadcastLog( isHeartMsg bool){

	rf.mu.Lock()
	if isHeartMsg == false{			//不是心跳消息
		rf.VotelogNum = 0		//初始化此条log 的赞同人数为0
	}

	//DPrintf("%v broadcast msg\n",rf.me)
	rf.mu.Unlock()

	for i,_ := range rf.peers {
		if i!= rf.me {

			rf.mu.Lock()

			//初始化AppendEntries参数
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.CommitIndex,
			}
			if !isHeartMsg {		//不是心跳消息，正常的log日志消息
				//选择i结点对应的log信息，不一定是最新的
				tmpNextIndex := rf.NextIndex[i]
				args.Entries = append(args.Entries,rf.log[tmpNextIndex])
			}else{
				args.Entries = nil			//心跳消息只需要空的日志
			}

			tmpPreLogIndex:=rf.NextIndex[rf.me] - 1			//leader的上一个log index
			args.PrevLogIndex = tmpPreLogIndex
			args.PreLogTerm = rf.log[tmpPreLogIndex].Term		//Leader上一个log的term

			reply := AppendEntriesReply{}

			rf.mu.Unlock()
			go rf.BroadcastLog(i,&args,&reply)
		}
	}


}


////leader发送心跳消息
////进入此函数时，已经上锁了
//func (rf *Raft)HeartBeatMsg(server int, args * AppendEntriesArgs, reply *AppendEntriesReply,wg *sync.WaitGroup){
//
//	if  rf.SendAppendEntries(server,args,reply) {
//		rf.mu.Lock()
//
//		if reply.Success {			//follower成功接受消息
//			//更新leader中保存的node信息，Lab 2A中暂不需要
//
//		}else{			//followers未成功接受消息，有可能leader的term较小
//
//			rf.CurrentTerm = reply.Term		//取较大的term
//			rf.InitFollower()		//转化为Followers
//			DPrintf("leader %v: send HeartMsg to %v failed and force to become follower",rf.me,server)
//
//		}
//
//		rf.mu.Unlock()
//	} else {			//未正常返回replay，有可能是因为等待超时
//		DPrintf("%v:receive no reply(log) from %v",rf.me,server)
//	}
//
//	if wg != nil{
//		wg.Done()
//	}
//
//
//}
//
////成为Leader之后，首先通知其他的结点
//func (rf *Raft)InitHeartMsg(){
//
//	//向除了自己的所有节点发送AppendEntries消息
//	for i, _ := range rf.peers {
//
//		if i != rf.me { //不需要给自己发送log消息
//
//			args := AppendEntriesArgs{
//				Term:         rf.CurrentTerm,
//				LeaderId:     rf.me,
//				PrevLogIndex: 0,
//				PreLogTerm:   0,
//				Entries:      nil,
//				LeaderCommit: 0,
//			}
//			reply := AppendEntriesReply{}
//
//			go rf.HeartBeatMsg(i, &args, &reply,nil)			//发送心跳消息
//		}
//	}
//
//}
//
////leader发送周期消息
//func (rf *Raft)PerMsg(){
//
//	tick:=100			//周期为tick
//	//wg:=sync.WaitGroup{}
//
//	//向除了自己的所有节点发送AppendEntries消息
//	for i, _ := range rf.peers {
//
//		if i != rf.me { //不需要给自己发送log消息
//
//			rf.mu.Lock()
//
//			args := AppendEntriesArgs{
//				Term:         rf.CurrentTerm,
//				LeaderId:     rf.me,
//				PrevLogIndex: 0,
//				PreLogTerm:   0,
//				Entries:      nil,
//				LeaderCommit: 0,
//			}
//			reply := AppendEntriesReply{}
//
//			rf.mu.Unlock()
//			go rf.HeartBeatMsg(i, &args, &reply,nil)			//发送心跳消息
//		}
//	}
//
//	time.Sleep(time.Millisecond*time.Duration(tick))		//休眠tick ms
//
//}
