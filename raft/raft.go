package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

const FOL_BASE_TIME = 300
const CAN_BASE_TIME = 150
const  RANDTIME = 300

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
	LastLogIndex	int 	// index of candidate's last log entry
	LastLogTerm		int 	// term of candidate's last log entry

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

//Log entry定义
type  LogEntry struct {
	Term 	int 		//每个log entry都带有term
	Index 	int			//当前的index
	Cmd		interface{}		//命令


}

type AppendEntriesArgs struct {
	Term int			//leader's term
	LeaderId int 		//so follower can redirect clients
	PrevLogIndex int	//index of log entry immediately preceding new ones
	PreLogTerm	int 	//term of PrevLogIndex entry
	Entries[] 	LogEntry	//log entries to store (empty for heartbeat; may send more than one for efficiency)
	//Entries 		LogEntry		//默认先处理一条log信息
	LeaderCommit	int	// leader's commitIndex

}

type AppendEntriesReply struct {
	Term	int		//currentTerm, for leader to update itself
	Success	bool	//true if follower contained entry matching preLogIndex and preLogTerm
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
	VotelogNum int 		//赞同log的数量

	//follower的相关计时
	elapsTime			int 			//follower状态周期计时的当前时间
	waitTime		int 			//follower状态需要等待的周期时间

	//candidate的相关计时
	//eleElaTime   	int 	//candidate 已等待时间
	eleWaitTime 	int 	//candidate的time out的时间
	voteNum	int		//获取的投票的数量

	//Persistent state on all servers
	CurrentTerm		int 	//Lastest term server has seen (initialized to 0,increases monotonically)
	VoteFor			int 	//candidateId that received vote in current term (or null if none)
	log[]			LogEntry			// log entries; each enray contains command for state machine, and term when entry was received by leader(first index is 1)

	//Volateile state on all  servers
	CommitIndex		int 	//index of highest log entry known to be committed (initialized to 0, increases monitonically)
	LastApplied		int 	// index of highest log entry applied to state machine(initialized to 0,increases monotonically)

	//volatile  state on leaders
	NextIndex[]		int		// for each server,index of the next log entry to send to that server (initialized to leader last log index+1)
	MatchIndex[]	int 	//for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monitonically)
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//////////////////////////以下是RPC相关处理//////////////////////////////

//处理leader发来的log请求（也有可能是heartbeat消息）
//？？？follower的最后一条日志，是由哪个变量来记录的？？？

func (rf *Raft) AppendEntries(args * AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//注意，此处还有很多参数未处理
	if len(args.Entries) == 0{		//log日志长度为0，代表的是heartbeat消息

		if args.Term >= rf.CurrentTerm {			//leader的周期大于当前周期
			rf.CurrentTerm = args.Term
			rf.InitFollower()		//转化为Followers
			reply.Success = true

			DPrintf("%v(statue:%v) recv HeartMsg from %v and force to become follower",rf.me,rf.role,args.LeaderId)
		}else {				//leader的周期小于当前周期
			reply.Term = rf.CurrentTerm			//返回当前term
			reply.Success = false

			DPrintf("%v recv HeartMsg from %v and reject it",rf.me,args.LeaderId)
		}
	}else {					//处理日志消息

		if args.Term < rf.CurrentTerm{		//leader的term较小
			reply.Term = rf.CurrentTerm			//返回当前term
			reply.Success = false

			DPrintf("%v recv AppendingEntries from %v and reject it",rf.me,args.LeaderId)
		}else{
			tmpPrevLogIndex := args.PrevLogIndex		//leader的上一个日志index
			tmpTerm:=args.Term							//leader的上一个日志term

			if rf.CommitIndex < tmpPrevLogIndex{		//follower的log相差leader较多，还没达到Leader的PrevLogIndex
				reply.Success = false

			}else {
					if rf.log[tmpPrevLogIndex].Term == tmpTerm { 		//index和term都符合
						//rf.log[args.PrevLogIndex+1]
						if cap(rf.log) == tmpPrevLogIndex+1{			//容量不够了
							rf.log = append(rf.log,args.Entries[0])		//只添加一条log命令
						}else {											//还有空余容量
							rf.log[tmpPrevLogIndex+1] = args.Entries[0]		//添加进rf.log
						}

						//更新rf.CommitIndex为min{args.LeaderCommit,tmpPrevLogIndex}
						if args.LeaderCommit > tmpPrevLogIndex{
							rf.CommitIndex = tmpPrevLogIndex
						}else{
							rf.CommitIndex = args.LeaderCommit
						}

						reply.Success = true		//记录成功
					} else {				//相同的index中存储着不同的term

						rf.CommitIndex = tmpPrevLogIndex-1			//follower的CommitIndex向前移，等待下一次通信时，进行覆盖
						reply.Success = false
					}
				}
			}
		}
}

//发送log消息（也有可能是heartbeat消息）给followers
func (rf *Raft) SendAppendEntries(server int, args * AppendEntriesArgs, reply *AppendEntriesReply)bool {

	DPrintf("%v send HeartMsg to %v",rf.me,server)
	ok:= rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

//判断candidate的log信息是否更新
//进入此函数时已经申请到锁
func (rf *Raft)UpToDate(args *RequestVoteArgs) bool{
	isNew:=false

	if args.LastLogTerm < rf.log[rf.CommitIndex].Term {
		isNew = false
	}else if args.LastLogTerm > rf.log[rf.CommitIndex].Term {
		isNew = true
	}else if args.LastLogTerm == rf.log[rf.CommitIndex].Term {
		if args.LastLogIndex >= rf.CommitIndex {
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

	rf.mu.Lock()			//加锁
	defer rf.mu.Unlock()

	vote:= false

	if args.Term<rf.CurrentTerm {		//如果candidate的Term较小
		vote = false
		DPrintf("test4:%v refuse to vote to %v",rf.me,args.CandidateId)

	}else if args.Term == rf.CurrentTerm {		//处在同一term中
		if rf.VoteFor != -1 {			//已经投票了
			vote = false
			DPrintf("test3:%v refuse to vote to %v",rf.me,args.CandidateId)
		}else {
			vote = true
			DPrintf("test2:%v vote to %v",rf.me,args.CandidateId)
		}
	} else {
		if rf.UpToDate(args) {			//candidate的日志是否更新
			vote = true
			DPrintf("test1:%v vote to %v",rf.me,args.CandidateId)
		}else {
			vote = false
			DPrintf("test5:%v refuse to vote to %v",rf.me,args.CandidateId)
		}

	}

	if vote {
		reply.VoteGranted = true		//投票

		rf.CurrentTerm = args.Term		//采取candidate的term
		rf.VoteFor = args.CandidateId		//本轮次已投票
		rf.InitFollower()		//转化为Followers
	}else{
		reply.VoteGranted = false		//拒绝投票
		reply.Term = rf.CurrentTerm		//返回当前周期
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
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
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
		log := LogEntry{
			Term:  rf.CurrentTerm,
			Index: 0,
			Cmd:   command,
		}

		var logs[] LogEntry
		logs = append(logs,log)

		//加入自己的log日志中
		rf.log = append(rf.log,logs[0])		//默认先添加第一条log信息
		rf.NextIndex[rf.me]++		//更新下一个log的index

		rf.InitBroadcastLog(false)//向其他的follow同步log信息

		//返回的信息
		index = rf.NextIndex[rf.me]
		term = rf.CurrentTerm
		isLeader = true

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
			//	rf.mu.Lock()
				rf.InitBroadcastLog(true)			//周期性发送心跳消息
			//	rf.mu.Unlock()
				//rf.PerMsg()
		}

		time.Sleep(time.Millisecond*time.Duration(10))		//休眠tick ms

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

	rf.VoteFor = -1					//并没参加选举
	rf.voteNum=0					//初始化投票的数量
	rf.CurrentTerm = 0				//初始化当前term
	rf.CommitIndex = 0				//初始化CommitIndex
	rf.LastApplied = 0				//初始化lastApplied

	rand.Seed(time.Now().UnixNano())		//每次从新设置随机数种子

	//初始化第一个log
	rf.log = append(rf.log,LogEntry{
		Term:  0,
		Index: 0,
		//Cmd:   "initCmd",
	})



	rf.InitFollower()			//初始化为Follower

	go rf.RaftRunning()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
