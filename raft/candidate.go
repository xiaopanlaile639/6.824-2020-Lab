package raft

import (
	"math/rand"
	"time"
)

//初始化Candidate的一些状态信息
//note:进入此函数时，已经获得了锁
func (rf* Raft) InitCandidate(){

	rf.role = CANDIDATE
	rf.VoteFor = rf.me				//投票给自己
	rf.CurrentTerm+=1		//周期数加1
	rf.voteNum = 0
	//设置candidate的超时时间
	rf.eleWaitTime =  rand.Intn(RANDTIME)+CAN_BASE_TIME//随机设置本term等待的时间：200ms-300ms之间

	rf.persist()				//持久化存储

	DPrintf("%v become candidate(%v)",rf.me,rf.CurrentTerm)
}


//向server请求投票
func (rf*Raft) AskVote(server int, args *RequestVoteArgs, reply *RequestVoteReply ){

	//发送rpc之前的term
	oriTerm:= args.Term

	if  rf.sendRequestVote(server,args,reply) {		//如果成功返回数据

		rf.UpdateTerm(reply.Term)			//先更新当前term

		//rf.mu.Lock()
		rf.lock("AskVote lock")

		if oriTerm == rf.CurrentTerm {		//如果term没有发生改变
			if reply.VoteGranted  { //接受投票

				if rf.role == CANDIDATE{		//如果还是Candidate，即还未被选为leader
					rf.voteNum++
					DPrintf("%v receive vote from %v\n",rf.me,server)
					if rf.voteNum >= (len( rf.peers) / 2){			//投票人数大于一半（加上自己的一票，只需要》=即可）
						rf.InitLeaderWithLock() //成为新的leader

						////立即发送一条心跳消息
						rf.unlock("AskVote lock")
						//rf.mu.Unlock()

						rf.BroadcastLog(true) //发送心跳消息通知其他节点，自己成为了leader
					//	rf.mu.Lock()
						rf.lock("AskVote lock")
					}
				}else{
					DPrintf("%v is not candidate now, it's role is %v\n",rf.me,rf.role)
				}

			}else{			//未获得投票,因为log不是最新的

				DPrintf("%v's term is smaller than me(%v),but my log is not up to date, ",server,rf.me)

				rf.InitFollowerWithLock(-1) //转化为Followers,不投票给任何人

			}
		}else{	//未获得投票，因为term比较小
			DPrintf("reply(%v)'s term(%v) is higher than my(%v)'s (%v) \n",server,reply.Term,rf.me,rf.CurrentTerm)
		}

		//rf.mu.Unlock()
		rf.unlock("AskVote lock")

	}else{				//未正常返回replay，有可能是因为等待超时(可能是由于网络不通，或者宕机)

		DPrintf("%v receive no reply(vote) from %v",rf.me,server)

	}
}

//向所有的server发起request 投票
//candidate如何记录上一条日志
//note:进入此函数时，已经拿到了锁
func (rf*Raft) VoteForMe(){

	//向除了自己的所有节点发送RequestVote消息
	for i,_:= range rf.peers {
		if i != rf.me{			//逻辑上需要为自己投票，实际上可以省略

			//rf.mu.Lock()
			rf.lock("VoteForMe lock")

			//请求信息
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateId:  rf.me,
			}

			tmpLastLog:=rf.GetLastLogEntryWithLock()
			args.LastLogIndex = tmpLastLog.Index	//上一条日志信息的index
			args.LastLogTerm = tmpLastLog.Term //上一条日志信息的term

			//rf.mu.Unlock()
			rf.unlock("VoteForMe lock")

			reply := RequestVoteReply{}
			go rf.AskVote(i,&args,&reply)		//在新线程里发起rpc投票

		}
	}

}

//开始一轮新的选举
func(rf*Raft) StartElection(){
	//rf.mu.Lock()

	rf.lock("StartElection lock")

	rf.InitCandidate()

	rf.unlock("StartElection lock")
	//rf.mu.Unlock()

	rf.VoteForMe()		//向所有的节点发起请求

	time.Sleep(time.Millisecond*time.Duration(rf.eleWaitTime))		//休息一会等待vote完毕 ms
}


