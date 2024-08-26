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
	// "fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           State
	lastHeartbeat   time.Time
	electionTimeout time.Duration
	votedFor        int
	currentTerm     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("[%s] AppendEntries leader %d -> %d with term %d\n", time.Now().Format("2001-01-01 00:00:00.000"), args.LeaderId, rf.me, args.Term)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		//fmt.Printf("[%s] AppendEntries %d switched from %v to Follower\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.state)
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	reply.Success = false
	if args.Term == rf.currentTerm {
		// If AppendEntries RPC received from new leader: convert to follower (If this node is Candidate with same term as current leader, convert to follower)
		if rf.state == Candidate {
			// fmt.Printf("[%s] AppendEntries %d switched to Follower from %v\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.state)
			rf.becomeFollower(args.Term)
		}

		reply.Success = true
		rf.resetElectionTimeout()
	}

	reply.Term = rf.currentTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("[%s] RequestVote %d -> %d\n", time.Now().Format("2001-01-01 00:00:00.000"), args.CandidateId, rf.me)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	reply.VoteGranted = false
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		//fmt.Printf("[%s] Vote granted from %d to %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeout()
	}

	reply.Term = rf.currentTerm
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	// //fmt.Printf("[%s] %d ticker started\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me)
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		if rf.state == Leader {
			// Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
			rf.sendHeartBeats()
		} else if (rf.state == Follower || rf.state == Candidate) && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			// If election timeout elapses without receiving AppendEntriesRPC from current leader or granting vote to candidate: convert to candidate
			//fmt.Printf("[%s] ticker %d electionTimeout: %v, time since heartbeat: %v\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.electionTimeout, time.Since(rf.lastHeartbeat))
			rf.startElection()
		}

		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

// called with mu locked
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimeout()

	//fmt.Printf("[%s] Node %d start election for term %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)

	voteCount := 1
	electionTerm := rf.currentTerm // save the term to avoid data race when constructing request vote args
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			args := &RequestVoteArgs{
				Term:        electionTerm,
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}
			response_ok := rf.sendRequestVote(peer, args, reply)

			if !response_ok {
				//fmt.Printf("[%s] RequestVote %d -> %d error\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				//fmt.Printf("[%s] RequestVote %d -> %d term outdated\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				rf.becomeFollower(reply.Term)
				return
			}

			if !reply.VoteGranted {
				//fmt.Printf("[%s] RequestVote %d -> %d term vote not granted\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				return
			}

			voteCount++
			if rf.state == Candidate && voteCount > len(rf.peers)/2 {
				//fmt.Printf("[%s] RequestVote %d win leader for term %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)
				rf.becomeLeader()
			}
		}(peer)
	}
}

// called with mu locked
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTimeout()
	//fmt.Printf("[%s] %d becomeFollower, term: %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)
}

// called with mu locked
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	//fmt.Printf("[%s] %d becomeLeader, term: %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)
}

func (rf *Raft) sendHeartBeats() {
	leaderTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			args := &AppendEntriesArgs{
				Term:     leaderTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			response_ok := rf.sendAppendEntries(peer, args, reply)

			if !response_ok {
				// //fmt.Printf("[%s] sendHeartBeats %d -> %d err\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				//fmt.Printf("[%s] sendHeartBeats %d -> %d term outdated\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				rf.becomeFollower(reply.Term)
				return
			}
		}(peer)
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
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

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
