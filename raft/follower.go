package raft

import (
	"math/rand"
	"time"
)

//Follower状态，等待称为Candidate
func (rf *Raft) WaitToBecCan(){

	tick:=20
	for {
		//rf.mu.Lock()
		rf.lock("WaitToBecCan lock")

		if rf.role == FOLLOWER {
			//等待超时
			if rf.elapsTime<=rf.waitTime {
				rf.elapsTime=rf.elapsTime+tick				//计时周期加tick ms

				//rf.mu.Unlock()	//睡眠之前解锁，否则可能会死锁
				rf.unlock("WaitToBecCan lock")

				time.Sleep(time.Millisecond*time.Duration(tick))		//休息tick ms

			}else{					//延时周期到，follower称为candidate

				rf.role = CANDIDATE
				//rf.InitCandidate()			//成为了Candidate

				//rf.mu.Unlock()
				rf.unlock("WaitToBecCan lock")
				break
			}
		}
	}
}

//初始化成为Follower的一些信息
//注意，进入此函数，应该已经获得了锁
func (rf* Raft) InitFollowerWithLock(VoteFor int){
	rf.role = FOLLOWER
	rf.elapsTime = 0
	rf.waitTime = rand.Intn(RANDTIME)+FOL_BASE_TIME	//设置下次超时的时间

	rf.VoteFor = VoteFor				//初始化成为Follower的时候还未投票

	rf.persist()				//持久化存储

	DPrintf("%v become follower\n",rf.me)

}