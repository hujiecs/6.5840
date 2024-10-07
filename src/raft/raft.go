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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

type LogEntry struct {
	Command interface{}
	Term    int
}

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
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
}

func (rf *Raft) DebugDump() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Id: %d, state: %v, currentTerm: %d, logLen: %d, commitIndex: %d, lastApplied: %d, log: %v\n", rf.me, rf.state, rf.currentTerm, len(rf.log), rf.commitIndex, rf.lastApplied, rf.log)
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Failed to read persist data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("[%s] AppendEntries leader %d -> %d with args %v, my term: %d, my loglen: %d, my log: %v\n", time.Now().Format("2001-01-01 00:00:00.000"), args.LeaderId, rf.me, args, rf.currentTerm, len(rf.log), rf.log)
	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		//fmt.Printf("[%s] AppendEntries %d switched from %v to Follower\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.state)
		rf.becomeFollower(args.Term)
	}

	if args.Term == rf.currentTerm {
		// If AppendEntries RPC received from new leader: convert to follower (If this node is Candidate with same term as current leader, convert to follower)
		if rf.state == Candidate {
			// fmt.Printf("[%s] AppendEntries %d switched to Follower from %v\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.state)
			rf.becomeFollower(args.Term)
		}

		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex > 0 && (args.PrevLogIndex > len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
			// when some node disconnects and rejoins, leader will have to decrement prevLogIndex several times
			// this node may timeout and have higher term, reset the timeout to avoid this.
			rf.resetElectionTimeout()
			reply.Success = false
			return
		}

		reply.Success = true

		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + i + 1
			if index <= len(rf.log) && rf.log[index-1].Term != entry.Term {
				// fmt.Printf("[%s] AppendEntries %d -> %d truncated log len from %d -> %d\n", time.Now().Format("2001-01-01 00:00:00.000"), args.LeaderId, rf.me, len(rf.log), index)
				rf.log = rf.log[:index-1]
				rf.persist()
				break
			}
		}

		// Append any new entries not already in the log
		for i := range args.Entries {
			index := args.PrevLogIndex + i + 1
			if index > len(rf.log) {
				// fmt.Printf("[%s] AppendEntries %d append entry %v to log\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, entry)
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		}

		//  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			min := args.LeaderCommit
			if len(rf.log) < min {
				min = len(rf.log)
			}
			rf.commitIndex = min
		}

		rf.resetElectionTimeout()
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	// fmt.Printf("[%s] RequestVote %d -> %d, my term: %d, lasLogIndex: %d, lastLogTerm:%d, args: %v, my log: %v\n", time.Now().Format("2001-01-01 00:00:00.000"), args.CandidateId, rf.me, rf.currentTerm, lastLogIndex, lastLogTerm, args, rf.log)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm (§5.1)
	reply.VoteGranted = false
	// check votedFor to avoid vote for multiple candidate with same term
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term == rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		//fmt.Printf("[%s] Vote granted from %d to %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader

	// fmt.Printf("[%s] Start agreement on Node %d, is leader? %v\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, isLeader)
	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		index = len(rf.log)
		rf.persist()
		// fmt.Printf("[%s] Append command %v to Node %d\n", time.Now().Format("2001-01-01 00:00:00.000"), command, rf.me)
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

		if rf.commitIndex > rf.lastApplied {
			lastApplied := rf.lastApplied
			entries := rf.log[rf.lastApplied:rf.commitIndex]
			// fmt.Printf("[%s] Node %d entries to apply: %v, indexs [%d - %d]\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, entries, rf.lastApplied, rf.commitIndex-1)
			rf.lastApplied = rf.commitIndex
			go rf.ApplyEntries(entries, lastApplied)
		}

		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// called with mu locked
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimeout()

	//fmt.Printf("[%s] Node %d start election for term %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)

	voteCount := 1
	electionTerm := rf.currentTerm // save the term to avoid data race when constructing request vote args
	lastLogIndex, lastLogTerm := rf.lastLogIndexTerm()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         electionTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
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
				// fmt.Printf("[%s] RequestVote %d win leader for term %d after response from %d at time %v\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm, peer, time.Now().Unix())
				rf.becomeLeader()
			}
		}(peer)
	}
}

// called with mu locked
func (rf *Raft) lastLogIndexTerm() (int, int) {
	if len(rf.log) > 0 {
		lastLogIndex := len(rf.log)
		lastLogTerm := rf.log[lastLogIndex-1].Term
		return lastLogIndex, lastLogTerm
	} else {
		return -1, -1
	}
}

// called with mu locked
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.resetElectionTimeout()
	//fmt.Printf("[%s] %d becomeFollower, term: %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)
}

// called with mu locked
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	//fmt.Printf("[%s] %d becomeLeader, term: %d\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, rf.currentTerm)
}

// called with mu locked
func (rf *Raft) sendHeartBeats() {
	// save term so it won't change for all heartbeats
	leaderTerm := rf.currentTerm
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[peer]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			if prevLogIndex > 0 {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}
			// Avoid data race if leader append new log
			entries := make([]LogEntry, len(rf.log[nextIndex-1:]))
			copy(entries, rf.log[nextIndex-1:])

			args := &AppendEntriesArgs{
				Term:         leaderTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			// fmt.Printf("[%s] sendHeartBeats %d -> %d with payload: %v\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer, args)
			reply := &AppendEntriesReply{}
			response_ok := rf.sendAppendEntries(peer, args, reply)

			if !response_ok {
				// fmt.Printf("[%s] sendHeartBeats %d -> %d err\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				// fmt.Printf("[%s] sendHeartBeats %d -> %d term outdated\n", time.Now().Format("2001-01-01 00:00:00.000"), rf.me, peer)
				rf.becomeFollower(reply.Term)
				return
			}

			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			if !reply.Success {
				// An optimization/hack to pass TestBackup3B: when leader and follower has many conflict,
				// decrement nextIndex one at a time might not to be able to reach agreement timely
				// instead of implementing the optimization mentioned in part 3C, we simply set it to 1
				// log already exists in follower will be ignored anyway.
				rf.nextIndex[peer] = 1 // rf.nextIndex[peer] = nextIndex - 1
				return
			}

			// If successful: update nextIndex and matchIndex for follower (§5.3)
			newNextIndex := nextIndex + len(entries)
			rf.nextIndex[peer] = newNextIndex
			rf.matchIndex[peer] = newNextIndex - 1

			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
			for N := rf.commitIndex + 1; N <= len(rf.log); N++ {
				if rf.log[N-1].Term != rf.currentTerm {
					continue
				}

				count := 1
				for peer = range rf.peers {
					if rf.matchIndex[peer] >= N {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = N
				}
			}
		}(peer)
	}
}

// call with mu locked
func (rf *Raft) ApplyEntries(entries []LogEntry, lastApplied int) {
	for i, entry := range entries {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: lastApplied + i + 1,
		}
		// fmt.Printf("Node %d apply command %v command index: %d\n", rf.me, entry.Command, lastApplied+i+1)
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = time.Duration(250+rand.Intn(350)) * time.Millisecond
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

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
