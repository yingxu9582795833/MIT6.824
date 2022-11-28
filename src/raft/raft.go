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

//思路
//1.需要先定义一台服务器的状态，可以根据论文定义
//2.然后是选举者选出来的流程
//初始化：先初始化所有服务器，然后服务器开始等待接收心跳包
//每个服务器有一个随机的心跳包接收时间，最开始到达的超时时间的服务器，向其他服务器发起投票请求
//其他服务器对比任期号回应投票请求
//3.搞懂test的调用流程，通过test搞懂需要实现的接口

//注意点
//如果候选人收到领导人的心跳包，会变为追随者，同时重置任期号
import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

//---------自定义的一些类型 -----------------------
const (
	Follower Status = iota
	Candidate
	Leader
)
const (
	rejected State = iota
	voted
	used
	timeout
	success
	lose
	notLeader
)

type State int
type Status int

//心跳包

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	State State
	Term  int
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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

	//持久性状态
	currentTerm int
	votedFor    int
	log         []string

	//易失性状态
	commitIndex int
	lastApplied int

	//领导的状态
	nextIndex  []int
	matchIndex []int

	//自定义状态
	status           Status       //服务器的身份
	heartTime        int          //心跳时间
	electionOverTime int          //选举超时时间
	loseTime         int          //失联时间
	voteNum          int          //票数
	heartTicker      *time.Ticker //心跳定时器
	electionTimer    *time.Timer  //选举计时器
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == Leader
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CurrentTerm int //当前任期
	Ind         int //服务器索引
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	State State
	Term  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//如果当前任期大于收到任期，则不投票
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CurrentTerm < rf.currentTerm {
		reply.State = rejected
		DPrintf(Func{fType: RequestVote, op: Rejected}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		return
	} else if rf.votedFor != -1 && rf.currentTerm == args.CurrentTerm { //否则投票
		reply.State = used
	} else {
		//重置定时器
		rf.electionTimer.Reset(RandElection())
		DPrintf(Func{fType: RequestVote, op: Success}, args.Ind, rf.me, rf.currentTerm, args.CurrentTerm)
		rf.currentTerm = args.CurrentTerm
		rf.votedFor = args.Ind
		rf.status = Follower
		reply.State = voted
		reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//反复寻求投票
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		ticker := time.NewTicker(time.Duration(rf.loseTime) * time.Millisecond)
		for !ok {
			select {
			case <-ticker.C:
				reply.State = timeout
				break
			default:
				ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
			}
		}
	}
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		rf.electionTimer.Reset(RandElection())
		DPrintf(Func{fType: AppendEntries, op: Success}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteNum = 0
		reply.State = success
	} else {
		DPrintf(Func{fType: AppendEntries, op: Rejected}, args.LeaderId, rf.me, rf.currentTerm, args.Term)
		reply.State = rejected
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("%v sendAppendEntires to  start %v\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("%v sendAppendEntires to %v end \n", rf.me, server)
	if !ok {
		reply.State = lose
		return
	}
}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dead = 1
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	z := rf.dead
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTick() {
	for rf.killed() == false {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.status == Leader {
			rf.mu.Unlock()
			continue
		}
		//改变身份
		rf.status = Candidate
		//任期递增
		rf.currentTerm++
		//投票给自己
		rf.voteNum++
		rf.votedFor = rf.me
		//重置超时时间
		//fmt.Println(rf.me, rf.currentTerm)
		rf.electionTimer.Reset(RandElection())
		DPrintf(Func{fType: electionTick, op: Start}, rf.me, -1, rf.currentTerm)
		rf.mu.Unlock()
		//请求其他服务器投票
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					CurrentTerm: rf.currentTerm,
					Ind:         rf.me,
				}
				reply := RequestVoteReply{}
				rf.mu.Unlock()
				rf.sendRequestVote(i, &args, &reply)
				switch reply.State {
				case timeout:
					DPrintf(Func{fType: electionTick, op: Lose}, rf.me, i)
				case rejected:
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.electionTimer.Reset(RandElection())
						DPrintf(Func{fType: electionTick, op: Rejected}, rf.me, i, reply.Term, rf.currentTerm)
						rf.status = Follower
						rf.currentTerm = reply.Term
						rf.voteNum = 0
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				case voted:
					rf.mu.Lock()
					rf.voteNum++
					DPrintf(Func{fType: electionTick, op: Success}, rf.me, i, reply.Term, rf.currentTerm, rf.voteNum)
					if rf.voteNum > len(rf.peers)/2 {
						DPrintf(Func{fType: electionTick, op: BeLeader}, rf.me, i, reply.Term, rf.currentTerm, rf.voteNum, len(rf.peers))
						rf.status = Leader
					}
					rf.mu.Unlock()
				case used:
					rf.mu.Lock()
					DPrintf(Func{fType: electionTick, op: Used}, rf.me, i)
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

func (rf *Raft) heartTick() {
	for rf.killed() == false {
		<-rf.heartTicker.C
		//DPrintf(Func{fType: Test, op: Success}, rf.me)
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		//向其他的服务器发送心跳包
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.status != Leader {
					return
				}
				switch reply.State {
				case notLeader:
				case lose:
					DPrintf(Func{fType: heartTick, op: Lose}, rf.me, i, reply.Term, rf.currentTerm)
				case success:
					DPrintf(Func{fType: heartTick, op: Success}, rf.me, i, reply.Term, rf.currentTerm)
				case rejected:
					DPrintf(Func{fType: heartTick, op: Rejected}, rf.me, i, reply.Term, rf.currentTerm)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.electionTimer.Reset(RandElection())
						rf.voteNum = 0
						rf.votedFor = -1
					}
				}
			}(i)
		}
	}
}
func (rf *Raft) ticker() {
	go rf.heartTick()
	go rf.electionTick()
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
	rf.mu.Lock()
	// Your initialization code here (2A, 2B, 2C).

	rf.votedFor = -1
	rf.voteNum = 0
	rf.status = Follower
	rf.electionTimer = time.NewTimer(RandElection())
	rf.heartTicker = time.NewTicker(time.Duration(100) * time.Millisecond)
	rf.loseTime = 200
	rf.currentTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()
	// start ticker goroutine to start elections
	rf.ticker()

	return rf
}
