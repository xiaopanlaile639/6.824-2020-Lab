package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"

	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
 //import "bytes"
 import "../labgob"



//
// as each Raft peer becomes aware that successive Logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Logs entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const (
	FOLLOWER int = -1
	CANDIDATE      int = 0
	LEADER     int = 1
)

//const FOL_BASE_TIME = 300
const FOL_BASE_TIME =300
const CAN_BASE_TIME = 300
const  RANDTIME = 150


//
// as each Raft peer becomes aware that successive Logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Logs entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term	int 	//candidate's term
	CandidateId	int 	//candidate's requesting vote
	LastLogIndex	int 	// index of candidate's last Logs entry
	LastLogTerm		int 	// term of candidate's last Logs entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 	int 	//currentTerm,for candidate to update itself
	VoteGranted	bool	// true means candidate received vote
}

//Logs entry定义
type  LogEntry struct {
	Term 	int 		//每个log entry都带有term
	Index 	int			//当前的index
	Cmd		interface{}		//命令

	//VoteNum	int 			//记录目前接受的投票数量

}

type AppendEntriesArgs struct {
	Term int			//leader's term
	LeaderId int 		//so follower can redirect clients
	PrevLogIndex int	//index of Logs entry immediately preceding new ones
	PreLogTerm	int 	//term of PrevLogIndex entry
	Entries[] 	LogEntry	//Logs entries to store (empty for heartbeat; may send more than one for efficiency)
	//Entries 		LogEntry		//默认先处理一条log信息
	LeaderCommit	int	// leader's commitIndex

}

type AppendEntriesReply struct {
	Term	int		//currentTerm, for leader to update itself
	Success	bool	//true if follower contained entry matching preLogIndex and preLogTerm

	ConflictIndex	int 		//用作quickly backup
	ConfilctTerm	int 		//用作quickly backup
}



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role int //当前本server所处的状态：follower、candidate、leader
	applyCh	chan ApplyMsg		//通道用于 applyMsg
	Applycond *sync.Cond		//条件变量，用于等待apply条件的发生

	//VotelogNum int 		//赞同log的数量

	//follower的相关计时
	elapsTime			int 			//follower状态周期计时的当前时间
	waitTime		int 			//follower状态需要等待的周期时间

	//candidate的相关计时
	//eleElaTime   	int 	//candidate 已等待时间
	eleWaitTime 	int 	//candidate的time out的时间
	voteNum	int		//获取的投票的数量

	//Persistent state on all servers
	CurrentTerm int                    //Lastest term server has seen (initialized to 0,increases monotonically)
	VoteFor     int                    //candidateId that received vote in current term (or null if none)
	Logs        []			LogEntry // Logs entries; each enray contains command for state machine, and term when entry was received by leader(first index is 1)

	//Volateile state on all  servers
	CommitIndex		int 	//index of highest Logs entry known to be committed (initialized to 0, increases monitonically)
	LastApplied		int 	// index of highest Logs entry applied to state machine(initialized to 0,increases monotonically)

	//volatile  state on leaders
	NextIndex[]		int		// for each server,index of the next Logs entry to send to that server (initialized to leader last Logs index+1)
	MatchIndex[]	int 	//for each server, index of highest Logs entry known to be replicated on server(initialized to 0, increases monitonically)
}

/////////////////////////以上是相关数据结构定义///////////////////////////////////////////////

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//返回节点的状态信息，应该是用于test测试
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.role == LEADER{
		isleader = true
	}else{
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var CurTerm int
	var VoteFor int
	var Logs[]	LogEntry
	if d.Decode(&CurTerm) != nil ||  d.Decode(&VoteFor) != nil || d.Decode(&Logs) != nil {
	  DPrintf("Decode error\n")
	} else {
		rf.CurrentTerm = CurTerm
		rf.VoteFor = VoteFor
		rf.Logs = Logs
		DPrintf("Decode OK and recover log is:\n")

		rf.ShowLog()
	}
}

//////////////////////////以下是RPC相关处理//////////////////////////////

//处理leader发来的log请求（也有可能是heartbeat消息）

func (rf *Raft) AppendEntries(args * AppendEntriesArgs, reply *AppendEntriesReply) {

	isUpdated := rf.UpdateTerm(args.Term) //更新term

	isUpdateCommit := false
	reply.Success = false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//如果两个term相等，说明有可能在前面的UpdateTerm中进行了修改，即
	//leader的term大于或等于rf.CurrentTerm,就说明这个rf承认了leader的地位
	if args.Term == rf.CurrentTerm{

		if isUpdated == false {		//如果未被更新，说明原本就是同一周期
			rf.InitFollowerWithLock(rf.VoteFor)		//已经投票
		}

		tmpPrevLogIndex := args.PrevLogIndex		//leader的上一个日志index
		tmpTerm:=args.PreLogTerm							//leader的上一个日志term
		logOk:= (tmpPrevLogIndex <len(rf.Logs)) && (rf.Logs[tmpPrevLogIndex].Term == tmpTerm)	///???

		if  logOk{ //index和term都符合

			//DPrintf("leader's(%v) last log match with %v's(%v)\n",args.LeaderId,rf.me,rf.Logs[tmpPrevLogIndex].Cmd)

			reply.Success = true
			isConfilct:=false

			localLogIndex :=tmpPrevLogIndex+1
			leaderLogIndex :=0
			for ; leaderLogIndex <len(args.Entries); leaderLogIndex++{

				if  localLogIndex < len(rf.Logs) { //在小于本地日志长度范围内比较
					if rf.Logs[localLogIndex].Term != args.Entries[leaderLogIndex].Term{ //判断是否冲突
						isConfilct = true
						break				//发生冲突退出
					}
				}else {					//超过了范围
					break
				}
				localLogIndex++ //本地日志向后推移
			}

			if isConfilct{				//如果有冲突
				rf.Logs = rf.Logs[:localLogIndex] //删除所有冲突的元素
			}

			//复制剩下的元素
			for ; leaderLogIndex <len(args.Entries); leaderLogIndex++{
				rf.Logs = append(rf.Logs,args.Entries[leaderLogIndex])
			}

			//更新CommitIndex
			if args.LeaderCommit > rf.CommitIndex {
				isUpdateCommit = true
				tmpLastEntryIndex:= rf.GetLastLogEntryWithLock().Index
				//tmpLastEntryIndex:=len(rf.Logs)-1

				if tmpLastEntryIndex < args.LeaderCommit{
					rf.CommitIndex = tmpLastEntryIndex
				}else {
					rf.CommitIndex = args.LeaderCommit
				}
				DPrintf("%v's commitIndex is %v\n",rf.me,rf.CommitIndex)
			}
			rf.persist()			//持久化存储

		}else{			//log and term 不匹配

			if len(rf.Logs) <= tmpPrevLogIndex{				//node结点的日志小于preLogIndex
				reply.ConfilctTerm = -1		//没有冲突的term
				reply.ConflictIndex = len(rf.Logs)
			}else{				//term不匹配
				reply.ConfilctTerm = rf.Logs[tmpPrevLogIndex].Term		//返回冲突的term

				//寻找第一个term的冲突index
				for _,entry:= range rf.Logs{
					if entry.Term == reply.ConfilctTerm{
						reply.ConflictIndex = entry.Index
						break
					}
				}

			}

			DPrintf("leader's(%v) last log not match with %v's\n",args.LeaderId,rf.me)

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

//判断candidate的log信息是否更新
//进入此函数时已经申请到锁
func (rf *Raft)UpToDate(args *RequestVoteArgs) bool{
	isNew:=false

	tmpLastIndex:=len(rf.Logs)-1

	if args.LastLogTerm < rf.Logs[tmpLastIndex].Term {
		isNew = false
	}else if args.LastLogTerm > rf.Logs[tmpLastIndex].Term {
		isNew = true
	}else if args.LastLogTerm == rf.Logs[tmpLastIndex].Term {
		if args.LastLogIndex >= tmpLastIndex {
			isNew = true
		}else{
			return false
		}
	}

	return isNew
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.UpdateTerm(args.Term)			//不管三七二十一，只要是对方的term较高，就采用对方的

	rf.mu.Lock()			//加锁
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm

	if args.Term<rf.CurrentTerm {		//如果candidate的Term较小
		reply.VoteGranted = false
		DPrintf("test4:%v refuse to vote to %v",rf.me,args.CandidateId)		/////???
	}else if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		if rf.UpToDate(args){			//如果更新
			reply.VoteGranted = true				//投票

			rf.InitFollowerWithLock(args.CandidateId)     //转化为Followers,并投票给CandidateId
			DPrintf("test2:%v vote to %v",rf.me,args.CandidateId)
		}else{
			reply.VoteGranted = false
			DPrintf("test3:%v refuse to vote to %v",rf.me,args.CandidateId)		/////???
		}
	}else{
		reply.VoteGranted = false
		DPrintf("test1:%v refuse to vote to %v",rf.me,args.CandidateId)		/////???
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
//需要我们来保证sendRequestVote的时候是在另一个goruntine
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
//////////////////////////以上是RPC相关处理//////////////////////////////

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).

	rf.mu.Lock()
	if rf.role == LEADER {			//只有Leader才接受请求

		logLen:=len(rf.Logs)
		DPrintf("%v received cmd(%v-%v-%v) from client\n",rf.me,logLen,rf.CurrentTerm,command)

		//返回的信息
		//index = rf.NextIndex[rf.me]		//和下面语句的作用应该一样
		index = len(rf.Logs)		//此log所在的位置
		term = rf.CurrentTerm
		isLeader = true
		//构造日志
		log := LogEntry{
			Term:  rf.CurrentTerm,
			Index: index,
			Cmd:   command,
		}
		//加入自己的log日志中
		rf.Logs = append(rf.Logs,log) //添加一条log信息

		rf.persist()

		//以下两个语句不知是否有必要，（leader即是否有必要更新自己的nextIndex和matchIndex）
		rf.NextIndex[rf.me] = len(rf.Logs)             //更新下一个log的index
		rf.MatchIndex[rf.me]=rf.NextIndex[rf.me]-1			//更新leader的已复制log标志


		rf.mu.Unlock()
		rf.BroadcastLog(false) //向其他的follow广播log消息
		rf.mu.Lock()
	}else {
		isLeader = false
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
func (rf *Raft) RaftRunning(){

	for{
		rf.mu.Lock()			//例程会花费大量的时间耗在这儿
		role:= rf.role
		rf.mu.Unlock()

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

		//
	}
}

//提交log线程
func (rf* Raft) doCommit() {
	for{
		rf.mu.Lock()
		//如果CommitIndex<=LastApplied的话，就循环等待???(为啥要循环等待呢)？？？
		for rf.CommitIndex <= rf.LastApplied {
			rf.Applycond.Wait()
		}

		DPrintf("in Commit: %v's log len:%v",rf.me,len(rf.Logs))

		i:=rf.LastApplied+1
		for ; i<=rf.CommitIndex;i++ {
			DPrintf("%v apply Index %v(%v)  \n",rf.me,i,rf.Logs[i].Cmd)

			//？？？这里是log[i].Cmd还是log[i-1].Cmd
			msg:=ApplyMsg{true,rf.Logs[i].Cmd,i}
			rf.mu.Unlock()
			rf.applyCh<-msg
			rf.mu.Lock()
			rf.LastApplied = i
		}

		rf.mu.Unlock()

	}
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//rand.Seed(time.Now().UnixNano())		//设置随机数种子
	rf.Applycond = sync.NewCond(&rf.mu)		//给条件变量绑定锁

	rf.applyCh = applyCh			//将参数通道复制给rf中
	rf.VoteFor = -1					//并没参加选举
	rf.voteNum=0					//初始化投票的数量

	rf.CurrentTerm = 0				//初始化当前term
	rf.CommitIndex = 0				//初始化CommitIndex
	rf.LastApplied = 0				//初始化lastApplied

	rand.Seed(time.Now().UnixNano())		//设置随机数种子

	//初始化第一个log
	rf.Logs = append(rf.Logs,LogEntry{
		Term:  0,
		Index: 0,
		Cmd:   "initCmd",
	})

	DPrintf("%v init...\n",me)
	rf.role = FOLLOWER
	rf.elapsTime = 0
	rf.waitTime = rand.Intn(RANDTIME)+FOL_BASE_TIME	//设置下次超时的时间

	//rf.InitFollowerWithLock(-1) //初始化的时候，不能使用这个函数，因为这个函数中会有持久化操作

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.RaftRunning()			//实例运行
	go rf.doCommit()			//新开一个提交的单独例程



	return rf
}

//互斥返回结点的role
func (rf* Raft) Role() int {
	var role int
	rf.mu.Lock()
	role = rf.role
	rf.mu.Unlock()
	return role
}

//获得最后一条日志
//进入此函数时，已经获得锁
func (rf* Raft) GetLastLogEntryWithLock() LogEntry {
	entry := LogEntry{}
	if len(rf.Logs) == 0{
		entry.Term = rf.CurrentTerm
		entry.Index = 0
	} else {
		entry = rf.Logs[len(rf.Logs) - 1]
	}
	return  entry
}

//更新周期，用于request和response中
func (rf* Raft) UpdateTerm(term int) bool{

	isUpdated:= false
	rf.mu.Lock()
	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.InitFollowerWithLock(-1)				//不投票给任何server
		isUpdated = true
	}
	rf.mu.Unlock()
	return isUpdated		//返回是否进行了term更新，也反映了当前node是否被强制成为了FOLLOWER
}

func (rf* Raft) ShowLog(){

	for _,log:= range  rf.Logs{
		DPrintf("node(%v) has log:%v(term) %v(index) %v(cmd)",rf.me,log.Term,log.Index,log.Cmd)
	}
}