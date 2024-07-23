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

// 整体代码思路：
// 2A和2B的实现沿袭了Frans教授的实现思路，但2C和2D部分是自己的思路
// 函数名后加了L的代表调用环境是已经上锁了的

// 实现2D时，尤其注意死锁的问题
// 如果出现2D调用了太多次的insatllsnap传递，请注意跟随者的log[0]处的日志的term是否正确
// 实现时多使用日志输出，多看输出的信息，找到出问题的地方

// 实现2C时，尤其注意要认真实现figure8的各种要求
// 此外还要注意回退nextIndex的处理，不要使用逐步回退。否则时间太长过不了测试点
// 还要注意是否实现了领导人选举限制的要求

// 实现2B时
// 要保证下标不越界的情况下，才能进行日志判断（即领导者的日志条目可能远大于跟随者）

// 关于2D
// 来自服务器的index是全局索引
// 存储在node节点的lastIncludedIndex也是全局索引
// nextIndex和matchIndex也是全局索引
// commitIndex和lastAppliedIndex都是全局索引
// 对于每个节点使用的索引是局部索引
// 局部索引和全局索引之间通过lastIncludedIndex进行转化
// 也就是说，从lab2c转化为lab2d，除了快照之外，还需要做的一件重要的事，就是将所有使用类似于 len(rf.log)、rf.log[index]的地方都需要转化
// 此外，每个节点会单独进行快照，故而领导者只需要发现跟随者落后的情况下用InstallSnapShot进行同步

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
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

	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

type Log struct {
	Term     int
	Opertion interface{}
}

type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term         int
	Success      bool
	ConfictIndex int
	ConfictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Offset            int
	Data []byte
	// Done bool
	// 因为我的设计会有一个占位的日志条目
	// 这个参数用于传递快照提交的最后一个日志条目
	// 用于prevLog的检测
	LastIncludedCmd interface{}
}

type InstallSnapshotReply struct {
	Term int
}

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	logTemp := make([]Log, len(rf.log)-1)
	copy(logTemp, rf.log[1:])
	e.Encode(logTemp)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()

	rf.persister.SaveStateAndSnapshot(data, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Log
	var lastIncludedIndex int
	var lastIncludedTerm int

	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{Term: 0})

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = append(rf.log, log...)

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%v: snapShot from service, and the snapshot is %v\n", rf.me, snapshot)

	if rf.commitIndex < index || rf.lastIncludedIndex >= index {
		DPrintf("server %v 拒绝了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	// fmt.Printf("%v: SnapShot, index=%v, rf.commitIndex=%v, before lastIncludedIndex=%v, rf.lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)

	// 保存snapshot
	rf.snapShot = snapshot

	rf.lastIncludedTerm = rf.log[rf.localLogIndexL(index)].Term
	// 截断log
	rf.log = rf.log[rf.localLogIndexL(index):] // index位置的log被存在0索引处

	// index来自于服务器，为全局索引
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("server %v 读取快照失败: 无快照\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v 读取快照c成功\n", rf.me)
}

// 下面两个函数用于lab2d
// 因为lab2d中涉及到snapshot，即需要将已经快照处理后的日志删除，其中需要考虑到不同节点之间的InstallSnapShot的日志索引
// 我的处理是将下标分为全局坐标和只用于log的索引
// 需要这两个函数进行转化
func (rf *Raft) localLogIndexL(vIndex int) int {
	// 调用该函数需要是加锁的状态
	return vIndex - rf.lastIncludedIndex
}
func (rf *Raft) globalLogIndexL(rIndex int) int {
	// 调用该函数需要是加锁的状态
	return rIndex + rf.lastIncludedIndex
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
		rf.persist()
	}
	reply.Term = rf.currentTerm
}

// 处理附加日志RPCs
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.setElectionTime()

	// 如果自身日志条目为空------将所有日志条目复制过来即可
	// 如果prevLogIndex为负数------说明自身的nextIndex为空，也说明自身的有效的日志条目为空，将所有日志条目复制过来即可
	// 如果都不，则需要判断当前索引下的日志条目的任期是否相同
	// 如果相同---说明前面的日志条目都相同，则将传入的日志条目添加到后面-----需要处理prevLogIndex在中间的情况
	// 如果不相同，则返回false
	// 领导人的prev日志长度要不大于该节点的日志长度

	reply.Success = false
	canApply := false

	if len(args.Entries) == 0 {
		rf.resetRoleL(args.Term)
	}

	if args.PrevLogIndex <= rf.globalLogIndexL(len(rf.log)-1) && rf.log[rf.localLogIndexL(args.PrevLogIndex)].Term == args.PrevLogTerm {

		reply.Success = true

		rf.manageLogAppendL(args.PrevLogIndex, args.Entries)

		rf.persist()
	} else {
		if args.PrevLogIndex <= rf.globalLogIndexL(len(rf.log)-1) { //任期冲突
			reply.ConfictTerm = rf.log[rf.localLogIndexL(args.PrevLogIndex)].Term
		} else {
			reply.ConfictTerm = -1
		}

		reply.ConfictIndex = len(rf.log)
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.ConfictTerm {
				reply.ConfictIndex = i
				break
			}
		}
		reply.ConfictIndex = rf.globalLogIndexL(reply.ConfictIndex)
	}

	// 如果是-1 ，则说明term不匹配，返回的是最后一个日志的索引
	// 否则，返回该term的首个索引位置
	rf.commitIndex = min(args.LeaderCommit, rf.globalLogIndexL(len(rf.log)-1))

	// 检测是否可以“保护提交日志”
	if args.LeaderCommit < rf.globalLogIndexL(len(rf.log)) && rf.localLogIndexL(rf.commitIndex) >= 0 && rf.log[rf.localLogIndexL(rf.commitIndex)].Term == args.Term {
		canApply = true
	}

	if canApply {
		rf.applyCond.Signal()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.setElectionTime()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.resetRoleL(args.Term)
	}

	hasEntry := false

	for rIndex := 0; rIndex < len(rf.log); rIndex++ {
		if rf.globalLogIndexL(rIndex) == args.LastIncludedIndex && rf.log[rIndex].Term == args.LastIncludedTerm {
			hasEntry = true
			break
		}
	}

	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		rf.log = rf.log[rf.localLogIndexL(args.LastIncludedIndex):]
	} else {
		rf.log = make([]Log, 0)
		rf.log = append(rf.log, Log{Term: args.LastIncludedTerm, Opertion: args.LastIncludedCmd})
	}

	// 0位置的Index是最后一个

	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	go func() {
		rf.applyCh <- *msg
	}()
	rf.persist()
	rf.setElectionTime()
}

func (rf *Raft) manageLogAppendL(prevLogIndex int, entries []Log) {
	// 处理日志复制，包括冲突的情况
	if prevLogIndex != rf.globalLogIndexL(len(rf.log)-1) {
		logEntriesT := rf.log[0:rf.localLogIndexL(prevLogIndex+1)]
		rf.log = make([]Log, rf.localLogIndexL(prevLogIndex+1))
		copy(rf.log, logEntriesT)
	}
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) Checklog(canLastLogIndex int, canLastLogTerm int) bool {
	lastLogIndex := rf.globalLogIndexL(len(rf.log))

	lastLogTerm := rf.log[rf.localLogIndexL(lastLogIndex-1)].Term
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
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("%v, send installSnapshot to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).

	if rf.role != Leader {
		isLeader = false
		term = rf.currentTerm
	} else {
		term = rf.currentTerm
		index = rf.globalLogIndexL(len(rf.log))
		rf.log = append(rf.log, Log{rf.currentTerm, command})
		rf.persist()
	}

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
		} else if time.Now().After(rf.electiontime) { // 达到随机选举超时时间，开始选举
			// 重新设置随机选举超时时间
			rf.setElectionTime()
			// 发送选举请求

			// 设置自身状态
			rf.currentTerm += 1
			rf.role = Candidate
			rf.votedFor = rf.me
			rf.persist()
			rf.requestVotePreL()
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) requestVotePreL() {
	// 准备发送请求变量
	lastLogIndex := rf.globalLogIndexL(len(rf.log))
	lastLogTerm := rf.log[rf.localLogIndexL(lastLogIndex-1)].Term

	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	votes := 0
	for i := range rf.peers {
		// 并发执行，传递引用，需要在内部使用锁控制，因为votes是共享内存
		go rf.requestVote(i, args, &votes)
	}
}
func (rf *Raft) appendEntryPreL(isHeartBeat bool) {

	for i := range rf.peers {
		if i != rf.me && isHeartBeat {

			var term int
			var leaderId int
			var prevLogIndex int
			var prevLogTerm int
			var entries []Log
			var leaderCommit int

			term = rf.currentTerm
			leaderId = rf.me

			// 初始拥有一个空日志条目，则nextIndex初始大小为1
			prevLogIndex = rf.nextIndex[i] - 1
			globalLastLogIndex := rf.globalLogIndexL(len(rf.log) - 1)

			// 即follower需要的日志的索引，小于领导者的快照保存的日志索引，也就是说，该日志已经不再领导者节点内entries
			// 故而需要转为发送InstallSnapShot
			// 由领导者检测出来，并且由领导者主动发送
			if prevLogIndex < rf.lastIncludedIndex {
				go rf.installSnapShotPre(i)
				continue
			}
			// 如果需要的日志存在，那么就正常发送
			prevLogTerm = rf.log[rf.localLogIndexL(prevLogIndex)].Term

			// 使用lastLogIndex判断是否需要有日志条目
			// 将nextIndex及后面的所有日志都附上
			// 如果nextIndex小于等于log的lastLogIndex，说明需要将日志传出去
			if rf.nextIndex[i] <= globalLastLogIndex {
				// 附加日志
				entries = make([]Log, globalLastLogIndex-rf.nextIndex[i]+1)
				entriesT := rf.log[rf.localLogIndexL(rf.nextIndex[i]):]
				copy(entries, entriesT)
			} else {
				// 发送心跳
				entries = make([]Log, 0)
			}
			leaderCommit = rf.commitIndex

			args := AppendEntryArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
			go rf.appendEntry(i, args)
		}
	}
}
func (rf *Raft) installSnapShotPre(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var args InstallSnapshotArgs
	if rf.role != Leader {
		return
	}

	args = InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Opertion,
	}
	go rf.installSnapshot(peer, args)
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
func (rf *Raft) installSnapshot(peer int, args InstallSnapshotArgs) {
	var reply InstallSnapshotReply
	ok := rf.sendInstallSnapshot(peer, &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.processInstallSnapShotReplyReplyL(&reply, peer)
	}
}

func (rf *Raft) processInstallSnapShotReplyReplyL(reply *InstallSnapshotReply, peer int) {
	if reply.Term > rf.currentTerm {
		rf.resetRoleL(reply.Term)
		return
	}
	rf.nextIndex[peer] = rf.globalLogIndexL(1)
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
		return
	}

	// 检测是否是过期的请求
	if args.Term == rf.currentTerm {
		if reply.Success {
			// 不使用nextIndex本身的值进行加减，防止某个延迟回复重复加多次，导致nextIndex过大
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1

			// 只有当前任期内的日志可以通过计数来提交，之前的日志只能通过当前日志的保护下提交
			rf.advanceCommitL()
		} else {
			rf.nextIndex[peer] = reply.ConfictIndex
			// 循环遍历，找到是否存在该任期号的最后一条日志，并返回索引
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConfictTerm {
					rf.nextIndex[peer] = rf.globalLogIndexL(i + 1)
					break
				}
			}
		}
	}
}

func (rf *Raft) advanceCommitL() {
	sortedMatchIndex := make([]int, len(rf.matchIndex))
	copy(sortedMatchIndex, rf.matchIndex)
	sortedMatchIndex[rf.me] = rf.globalLogIndexL(len(rf.log) - 1)
	sort.Ints(sortedMatchIndex)
	N := sortedMatchIndex[len(rf.peers)/2]

	// 日志中的任期和当前任期相同时，可以进行保护提交（Figure 8)
	if rf.log[rf.localLogIndexL(N)].Term == rf.currentTerm {
		if rf.role == Leader && N > rf.commitIndex && rf.log[rf.localLogIndexL(N)].Term == rf.currentTerm {
			rf.commitIndex = N
			rf.applyCond.Signal()
		}
	} else {
		// fmt.Printf("cannot commit log before\n")
		return
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.localLogIndexL(rf.lastApplied)].Opertion,
				CommandIndex: rf.lastApplied,
			}

			// 不管是像这样将applier作为单独一个线程，然后使用applyCond(条件变量)来协调各进程
			// 还是在合适的地方直接调用applier
			// 都可以，但唯一需要保证的是，在向applyCh中传递消息的时候，不能持有锁

			// 如果此处有锁，那么在lab2D会死锁
			// lab2D的snapshot调用机制是这样的
			// 当service从applyCh收到消息后，会检测收到的消息数量是否达到一定数量
			// 如果达到后，就会调用节点的SnapShot函数。
			// 而此时如果rf.applyCh <- applyMsg这行代码是锁住的，那么就会导致该节点无法再申请一个新的锁
			// 于是SnapShot函数等待applier()函数释放锁，而applier()函数等待service接收完applyCh中的消息，而service等待SnapShot函数返回值
			// 就形成了死锁

			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) newLeaderL() {
	rf.role = Leader

	// 修改nextIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.globalLogIndexL(len(rf.log))
		rf.matchIndex[i] = rf.lastIncludedIndex
	}

	//发送心跳确立权威
	rf.appendEntryPreL(true)
}

func (rf *Raft) resetRoleL(newTerm int) {
	DPrintf("%v, set role to follower %v", rf.me, newTerm)
	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = newTerm
	rf.persist()
	rf.setElectionTime()
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(200 * time.Millisecond)
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

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.currentTerm = 0
	rf.votedFor = -1

	// 添加一个空日志条目
	// 注意，使用了一个日志条目来占位，再进行Lab2c时，需要考虑到这个情况
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.role = Follower
	rf.setElectionTime()

	rf.snapShot = make([]byte, 0)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.globalLogIndexL(len(rf.log)) // raft中的index是从1开始的
	}
	// start ticker goroutine to start elections
	go rf.applier()
	go rf.ticker()

	return rf
}
