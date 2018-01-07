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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	leader = iota
	candidate
	follower
)

const heartBeats = 100

type LogEntry struct {
	Index       int
	CurrentTerm int
	Command     interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh     chan ApplyMsg
	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastTime        time.Time
	electionTimeout time.Duration

	quitCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
	// Your code here (2A).
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arguments strcture
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC resply strcture
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	SuitableIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf(1, "Request received from %v to %v, log is %v, args : %v\n", args.CandidateId, rf.me, rf.log, args)
	rf.lastTime = time.Now()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false

		return
	} else if args.Term > rf.currentTerm {
		DPrintf(1, "RequestVote %v out of date, %v to %v\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if rf.state != follower {
			rf.state = follower
			rf.createFollowerThread()
		}
	}

	var lastLogIndex, lastLogTerm int = 0, 0

	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].CurrentTerm
	}

	isUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		rf.votedFor = args.CandidateId
		DPrintf(1, "RequestVote :%v voted %v, log is : %v\n", rf.me, rf.votedFor, rf.log)
		reply.VoteGranted = true
	} else {
		// fmt.Printf("%v request failed, I have already voted %v\n", args.CandidateId, rf.votedFor)
	}

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf(1, "Append HeartBeats from %v , I'm %v, CurrentTerm is %v\n", args.LeaderId, rf.me, rf.currentTerm)
	rf.lastTime = time.Now()

	reply.Term = rf.currentTerm

	prevLogEntry := LogEntry{}
	logLength := len(rf.log)

	if args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {
		prevLogEntry.CurrentTerm = 0
		prevLogEntry.Index = 0
	} else if logLength >= args.PrevLogIndex && args.PrevLogIndex > 0 {
		prevLogEntry = rf.log[args.PrevLogIndex-1]
	}
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.SuitableIndex = 0

		return
	} else if args.Term >= rf.currentTerm {
		if rf.votedFor != -1 {
			DPrintf(1, "AppendRPC out of date, %v to %v\n", rf.currentTerm, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		if rf.state != follower {
			rf.state = follower
			rf.createFollowerThread()
		}
	}

	if prevLogEntry.CurrentTerm != args.PrevLogTerm {
		reply.Success = false
		reply.SuitableIndex = 0

		for i := args.PrevLogIndex - 1; i > 0 && logLength >= i; i-- {
			if rf.log[i-1].CurrentTerm == args.PrevLogTerm {
				reply.SuitableIndex = rf.log[i-1].Index
				break
			}
		}

		return
	}

	DPrintf(1, "Append args: %v, prev is %v\n", args, prevLogEntry)
	DPrintf(1, "Server %v(term:%v) AppendEntry from %v(term:%v), before : %v, startIndex: %v, appendEntries:%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log, args.PrevLogIndex, args.Entries)

	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		lastIndex := math.MaxInt32
		if len(args.Entries) > 0 {
			lastIndex = args.Entries[len(args.Entries)-1].Index
		}

		rf.commitIndex = Min(args.LeaderCommit, lastIndex)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	isLeader = rf.state == leader

	if isLeader {
		index = len(rf.log) + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.matchIndex[rf.me] = len(rf.log)
		rf.persist()
		DPrintf(-1, "Leader %v Start a command %v, term is %v, index is %v\n", rf.me, command, term, index)
	}

	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.quitCh)
	DPrintf(0, "Kill server %v, Term: %v, State: %v\n", rf.me, rf.currentTerm, rf.state)
}

func (rf *Raft) createLeaderThread() {
	rf.mu.Lock()
	DPrintf(0, "New Leader %v  Term is %v, State is %v, log is %v\n", rf.me, rf.currentTerm, rf.state, rf.log)
	rf.matchIndex = make([]int, len(rf.nextIndex))
	lastIndex := len(rf.log)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastIndex
		if i == rf.me {
			rf.matchIndex[i] = lastIndex
		}
	}

	term := rf.currentTerm

	rf.mu.Unlock()
	go func() {
		defer DPrintf(0, "Finish leader gorutine: %v\n", GoID())
		for {
			rf.mu.Lock()
			select {
			case <-rf.quitCh:
				rf.mu.Unlock()
				return
			default:

				DPrintf(2, "Leader %v, Term %v Goroutine Num: %v, me is %v, CommitIndex is %v, ApplyIndex is %v, log is %v\n",
					rf.me, rf.currentTerm, runtime.NumGoroutine(), GoID(), rf.commitIndex, rf.lastApplied, rf.log)

				if rf.state != leader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}

				serverNum := len(rf.peers)

				lastIndex := len(rf.log)

				for i := 0; i < serverNum; i++ {
					if i != rf.me {
						go func(i int) {

							rf.mu.Lock()

							if rf.state != leader {
								rf.mu.Unlock()

								return
							}
							args := &AppendEntriesArgs{}
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.LeaderCommit = rf.commitIndex
							if rf.nextIndex[i] > 0 {
								args.PrevLogIndex = rf.log[rf.nextIndex[i]-1].Index
								args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].CurrentTerm
								args.Entries = rf.log[rf.nextIndex[i]:lastIndex]
							} else {
								args.PrevLogIndex = 0
								args.PrevLogTerm = 0
								args.Entries = rf.log[:lastIndex]
							}

							reply := &AppendEntriesReply{}

							rf.mu.Unlock()

							now := time.Now()
							ok := rf.sendAppendEntries(i, args, reply)

							rf.mu.Lock()
							defer rf.mu.Unlock()

							DPrintf(2, "HeartBeats to %v result : %v, match: %v, spent %v s\n",
								i, ok, rf.matchIndex, time.Since(now).Seconds())

							if ok && reply.Success {
								rf.nextIndex[i] = lastIndex
								rf.matchIndex[i] = lastIndex
							} else if ok && !reply.Success {
								rf.nextIndex[i] = reply.SuitableIndex
							}

							if reply.Term > rf.currentTerm {
								DPrintf(2, "Leader %v back to Follower，from term %v to %v\n", rf.me, rf.currentTerm, reply.Term)

								rf.currentTerm = reply.Term
								rf.votedFor = -1
								defer rf.persist()
								if rf.state != follower {
									rf.state = follower
									rf.createFollowerThread()

									return
								}

							}
						}(i)
					}
				}

				matchIndex := make([]int, len(rf.matchIndex))
				copy(matchIndex, rf.matchIndex[:])
				sort.Ints(matchIndex)
				newCommitIndex := matchIndex[len(matchIndex)/2]

				var term int
				if newCommitIndex > 0 {

					if newCommitIndex > len(rf.log) {
						fmt.Println("Index out of range ", rf.me, newCommitIndex, rf.log)
						term = 0
					} else {
						term = rf.log[newCommitIndex-1].CurrentTerm
					}

				}

				if newCommitIndex > rf.commitIndex && term == rf.currentTerm {
					DPrintf(2, "Server %v ComitIndex update. %v To %v, matchIndex:\n", rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex)
					rf.commitIndex = newCommitIndex
				}

			}

			rf.mu.Unlock()

			time.Sleep(heartBeats * time.Millisecond)
		}
	}()
}

func (rf *Raft) createCandidateThread() {
	go func() {
		defer DPrintf(0, "Finish candidate goroutine: %v", GoID())
		rf.mu.Lock()
		rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)

		rf.currentTerm++
		DPrintf(0, "New Election: %v is Candidate,  goroutine is %v,  Term is %v, State is %v, log is %v\n",
			rf.me, GoID(), rf.currentTerm, rf.state, rf.log)
		rf.votedFor = rf.me
		rf.persist()

		var lastLogIndex, lastLogTerm int = 0, 0

		if len(rf.log) > 0 {
			lastLogIndex = rf.log[len(rf.log)-1].Index
			lastLogTerm = rf.log[len(rf.log)-1].CurrentTerm
		}

		serverNum := len(rf.peers)
		votes := make(chan bool, serverNum+1)
		votes <- true

		rf.lastTime = time.Now()
		rf.mu.Unlock()
		for i := 0; i < serverNum; i++ {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					if rf.state != candidate {
						rf.mu.Unlock()
						return
					}
					DPrintf(1, "SendRequestVote from %v to %v, CurrentTerm is %v, log is %v\n", rf.me, i, rf.currentTerm, rf.log)
					args := &RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					args.LastLogIndex = lastLogIndex
					args.LastLogTerm = lastLogTerm
					reply := &RequestVoteReply{}

					rf.mu.Unlock()

					ok := rf.sendRequestVote(i, args, reply)

					DPrintf(1, "Request vote from %v to %v reply : net,%v result，%v\n", rf.me, i, ok, reply.VoteGranted)

					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						DPrintf(1, "Candidate %v back to Follower，from term %v to %v\n", rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()

						if rf.state != follower {
							rf.state = follower
							rf.mu.Unlock()
							rf.createFollowerThread()

							return
						}
					}

					rf.mu.Unlock()

					if ok && reply.VoteGranted {
						votes <- true
					}
				}(i)
			}
		}

		for {
			rf.mu.Lock()

			select {
			case <-rf.quitCh:
				rf.mu.Unlock()
				return
			default:
				if rf.state != candidate {
					rf.mu.Unlock()
					return
				}

				duration := time.Since(rf.lastTime)
				if duration > rf.electionTimeout {
					rf.mu.Unlock()
					rf.createCandidateThread()

					return
				}

				if len(votes) > int(serverNum/2) {
					rf.state = leader
					rf.mu.Unlock()
					rf.createLeaderThread()

					return
				}
			}

			rf.mu.Unlock()

			time.Sleep(heartBeats * time.Millisecond / 10)
		}
	}()
}

func (rf *Raft) createFollowerThread() {
	go func() {
		defer DPrintf(0, "Finish follower goroutine: %v", GoID())
		for {
			rf.mu.Lock()
			select {
			case <-rf.quitCh:
				rf.mu.Unlock()
				return
			default:
				DPrintf(3, "Follower %v, Term %v, Goroutine Num: %v, me is %v, log is %v\n",
					rf.me, rf.currentTerm, runtime.NumGoroutine(), GoID(), rf.log)
				if rf.state != follower {
					rf.mu.Unlock()
					return
				}

				duration := time.Since(rf.lastTime)

				if duration > rf.electionTimeout {
					// fmt.Printf("Timeout %v convert to Candidate\n", rf.me)
					rf.state = candidate
					rf.mu.Unlock()
					rf.createCandidateThread()

					return
				}

			}
			rf.mu.Unlock()

			time.Sleep(heartBeats * time.Millisecond / 10)
		}
	}()

}

func (rf *Raft) createApplyThread() {
	go func() {
		for {
			rf.mu.Lock()
			select {
			case <-rf.quitCh:
				rf.mu.Unlock()
				return
			default:
				for rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					applyLog := rf.log[rf.lastApplied-1]

					DPrintf(0, "Apply command to the state machine.I'm %v, State is %v, Index is %v, Term is %v,"+
						"Command is %v, CommitIndex is %v, LastApply is %v, logs is %v\n",
						rf.me, rf.state, applyLog.Index, rf.currentTerm, applyLog.Command, rf.commitIndex, rf.lastApplied, rf.log)

					rf.mu.Unlock()
					rf.applyCh <- ApplyMsg{applyLog.Index, applyLog.Command, false, []byte{}}
					rf.mu.Lock()
				}
			}
			rf.mu.Unlock()

			time.Sleep(heartBeats * time.Millisecond / 10)
		}
	}()
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
	DPrintf(0, "Make a new Raft, address is %v\n", &rf)
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.lastTime = time.Now()

	rf.quitCh = make(chan bool)

	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.createFollowerThread()
	rf.createApplyThread()

	return rf
}

// for test
func (rf *Raft) setTimeOut(duration time.Duration) {
	rf.mu.Lock()
	rf.electionTimeout = duration
	rf.mu.Unlock()
}
