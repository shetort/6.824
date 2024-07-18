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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg
	// State
	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role         NodeRole
	electiontime time.Time
}

type Log struct {
	Term     int
	opertion interface{}
}

type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
	// PrevLogIndex int
	// PrevLogTerm  int
	// Entries      []Log
	// LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
// 处理投票请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.resetRoleL(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidatedId) && rf.Checklog(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidatedId
		rf.setElectionTime()
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetRoleL(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) Checklog(canLastLogIndex int, canLastLogTerm int) bool {

	lastLogIndex := len(rf.log)
	var lastLogTerm int
	if lastLogIndex == 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	if canLastLogTerm > lastLogTerm {
		return true
	}
	if canLastLogTerm == lastLogTerm {
		return canLastLogIndex >= lastLogIndex
	}
	return false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%v, send requestVote to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	DPrintf("%v, send appendEntry to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()

		DPrintf("%v, tick state %v", rf.me, rf.role)

		if rf.role == Leader {
			// 重新设置随机选举超时时间
			rf.setElectionTime()
			// 发送附加条目/心跳
			rf.appendEntryPreL(true)
		}
		// 达到随机选举超时时间，开始选举
		if time.Now().After(rf.electiontime) {
			// 重新设置随机选举超时时间
			rf.setElectionTime()
			// 发送选举请求

			// 设置自身状态
			rf.currentTerm += 1
			rf.role = Candidate
			rf.votedFor = rf.me

			rf.requestVotePreL()
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(50) * time.Millisecond)
	}
}

func (rf *Raft) requestVotePreL() {
	// 准备发送请求变量

	lastLogIndex := len(rf.log)
	var lastLogTerm int
	if lastLogIndex == 0 {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	votes := 0 // 判断投票人数
	for i := range rf.peers {
		go rf.requestVote(i, args, &votes) // 并发执行，传递引用，需要在内部使用锁控制，因为votes是共享内存
	}
}
func (rf *Raft) appendEntryPreL(isHeartBeat bool) {
	for i := range rf.peers {
		if i != rf.me && isHeartBeat {
			args := AppendEntryArgs{rf.currentTerm, rf.me}
			go rf.appendEntry(i, args)
		}
	}
}

func (rf *Raft) requestVote(peer int, args RequestVoteArgs, votes *int) {
	var reply RequestVoteReply
	// 发送投票请求
	ok := rf.sendRequestVote(peer, &args, &reply)

	//处理投票请求回复
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.processRequestVoteReplyL(&reply, args.Term, votes)
	}
}
func (rf *Raft) appendEntry(peer int, args AppendEntryArgs) {
	var reply AppendEntryReply
	// 发送附加日志
	ok := rf.sendAppendEntry(peer, &args, &reply)
	//处理回复
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.processAppendEntryReplyL(&reply, &args, peer)
	}
}

func (rf *Raft) processRequestVoteReplyL(reply *RequestVoteReply, argsTerm int, votes *int) {
	if reply.Term > rf.currentTerm {
		rf.resetRoleL(reply.Term)
	} else if argsTerm == rf.currentTerm && reply.VoteGranted {

		*votes += 1
		//如果大多数节点投票给该节点，那么转化为领导人
		if *votes > len(rf.peers)/2 {
			rf.newLeaderL()
		}
	}
}
func (rf *Raft) processAppendEntryReplyL(reply *AppendEntryReply, args *AppendEntryArgs, peer int) {
	if reply.Term > rf.currentTerm {
		rf.resetRoleL(reply.Term)
	} else if args.Term == rf.currentTerm {
		// 处理自身的例如提交日志等
		return
	}
}

func (rf *Raft) newLeaderL() {
	rf.role = Leader
	// 修改nextIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}

	//发送心跳确立权威
	rf.appendEntryPreL(true)
}

func (rf *Raft) resetRoleL(newTerm int) {
	DPrintf("%v, reset role to follower %v", rf.me, newTerm)
	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = newTerm
	rf.setElectionTime()
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(300 * time.Millisecond)
	rand.Seed(time.Now().UnixNano())
	ms := rand.Intn(300)
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electiontime = t
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.role = Follower
	rf.setElectionTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
