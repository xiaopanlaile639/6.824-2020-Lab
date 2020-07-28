/*此文件定义了raft系统公用的一些对象属性定义*/

package raft

import (
	"../labrpc"
	"sync"
	"time"
)

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


const (
	SnapshotType string = "SNAPSHOT"
	StatsType	string	="STATS"
)

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

//	Snapshot  	SnapShot		//传递给kv server 的snapshot
	CommandTerm		int 		//apply log的周期

	//	CommandTerm	int 		//提交的周期

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

	//CmdMethod		string
	//CmdArgs			[]string
	//VoteNum	int 			//记录目前接受的投票数量

}

type AppendEntriesArgs struct {
	Term int			//leader's term
	LeaderId int 		//so follower can redirect clients
	PrevLogIndex int	//index of Logs entry immediately preceding new ones
	PreLogTerm	int 	//term of PrevLogIndex entry
	Entries[] 	LogEntry	//Logs entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit	int	// leader's commitIndex

}

type AppendEntriesReply struct {
	Term	int		//currentTerm, for leader to update itself
	Success	bool	//true if follower contained entry matching preLogIndex and preLogTerm

	ConflictIndex	int 		//用作quickly backup
	ConfilctTerm	int 		//用作quickly backup
}

//install Snapshot 的参数
type InstallSnapArgs struct {
	Term int
	LeaderId	int
	LastIncludedIndex	int
	LastIncludedTerm	int

	Snapshot SnapShot		//用作广播的snapshot

	Done		bool

}

//install Snapshot的回复
type InstallSnapReplys struct {
	Term int
	Ok bool
}

//Snapshot 结构体
type SnapShot struct {
	DB map[string]string		//保存的数据库
	LastCmdIndexMap	map[int64]int64		//

	LastAppliedIndex		int
	LastAppliedTerm			int
	//SnapshotIndex	int		//保存log的编号
	//SnapshotTerm	int //最后一个log的term


}


//需要persist的状态
type Stats struct {
	CurrentTerm	int
	VoteFor	int
	Logs []LogEntry

	LastIncIndex	int
	LastIncTerm		int
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

	LastIncIndex	int
	LastIncTerm		int

	//isInstallingSnapshot []int

	lockName	string
	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time


}
