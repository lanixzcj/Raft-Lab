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
	"fmt"
	"labrpc"
	"math"
	"math/rand"
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
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Request received from %v to %v\n", args.CandidateId, rf.me)
	// fmt.Printf("I'm %v, CurrentTerm is %v\n", rf.me, rf.currentTerm)
	rf.lastTime = time.Now()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false

		return
	} else if args.Term > rf.currentTerm {
		// fmt.Printf("RequestVote out of date, %v to %v\n", rf.currentTerm, args.Term)
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
		// fmt.Printf("%v voted %v\n", rf.me, rf.votedFor)
		reply.VoteGranted = true
	} else {
		// fmt.Printf("%v request failed, I have already voted %v\n", args.CandidateId, rf.votedFor)
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Received HeartBeats from %v , I'm %v, CurrentTerm is %v\n", args.LeaderId, rf.me, rf.currentTerm)
	// fmt.Printf("Sender's Term is %v from %v\n", args.Term, args.LeaderId)
	rf.lastTime = time.Now()
	// fmt.Println(rf.log, rf.me)

	reply.Term = rf.currentTerm

	prevLogEntry := LogEntry{}

	if args.PrevLogIndex == 0 && args.PrevLogTerm == 0 {
		prevLogEntry.CurrentTerm = 0
		prevLogEntry.Index = 0
	} else if len(rf.log) >= args.PrevLogIndex && args.PrevLogIndex > 0 {
		prevLogEntry = rf.log[args.PrevLogIndex-1]
	}
	reply.Success = true

	if args.Term < rf.currentTerm || prevLogEntry.CurrentTerm != args.PrevLogTerm {
		reply.Success = false

		return
	} else if args.Term >= rf.currentTerm {
		// fmt.Printf("RequestVote out of date, %v to %v\n", rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1

		if rf.state != follower {
			rf.state = follower
			rf.createFollowerThread()
		}
	}

	rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := math.MaxInt32
		if len(args.Entries) > 0 {
			lastIndex = args.Entries[len(args.Entries)-1].Index
		}

		// fmt.Printf("AppendRpc :Server %v ComitIndex update. %v To %v\n", rf.me, rf.commitIndex, Min(args.LeaderCommit, lastIndex))
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
		rf.matchIndex[rf.me]++
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
}

func (rf *Raft) createLeaderThread() {
	rf.mu.Lock()
	fmt.Printf("New Leader %v  Term is %v, State is %v\n", rf.me, rf.currentTerm, rf.state)
	rf.matchIndex = make([]int, len(rf.nextIndex))
	lastIndex := len(rf.log)
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastIndex
		if i == rf.me {
			rf.matchIndex[i] = lastIndex
		}
	}
	rf.mu.Unlock()
	go func() {
		isOutOfDate := false
		for {
			rf.mu.Lock()
			if rf.state != leader || isOutOfDate {
				rf.mu.Unlock()
				break
			}

			// if rf.me == 4 {
			// 	fmt.Printf("I'm %v, State is %v, Term is %v logs is %v\n",
			// 		rf.me, rf.state, rf.currentTerm, rf.log)
			// }
			// fmt.Printf("%v is Leader, Term is %v, CommitIndex is %v, ApplyIndex is %v\n", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
			serverNum := len(rf.peers)

			isOutOfDateCh := make(chan bool)
			lastIndex := len(rf.log)

			var wg sync.WaitGroup

			for i := 0; i < serverNum; i++ {
				if i != rf.me {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						rf.mu.Lock()
						// fmt.Printf("SendHeartBeats from %v to %v\n", me, i)
						// fmt.Printf("I'm %v, CurrentTerm is %v\n", rf.me, rf.currentTerm)

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

						ok := rf.sendAppendEntries(i, args, reply)

						if len(isOutOfDateCh) > 0 {
							return
						}

						rf.mu.Lock()

						// fmt.Printf("HeartBeats to %v result : %v match: %v\n", i, ok, rf.matchIndex)

						if ok && reply.Success {
							rf.nextIndex[i] = lastIndex
							rf.matchIndex[i] = lastIndex
						} else if ok && !reply.Success {
							rf.nextIndex[i]--
						}

						// fmt.Printf("Me %v, Beats Reply Term %v ,CurrentTerm %v\n", rf.me, reply.Term, rf.currentTerm)
						if reply.Term > rf.currentTerm {
							// fmt.Printf("out Of Date\n")
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							if rf.state != follower {
								rf.state = follower
								rf.mu.Unlock()
								rf.createFollowerThread()
								isOutOfDateCh <- true

								return
							}
						}

						rf.mu.Unlock()
					}(i)
				}
			}

			rf.mu.Unlock()

			go func() {
				wg.Wait()

				rf.mu.Lock()
				matchIndex := make([]int, len(rf.matchIndex))
				copy(matchIndex, rf.matchIndex[:])
				// fmt.Printf("Leader %v receive all server, MatchIndex is %v\n", rf.me, matchIndex)
				sort.Ints(matchIndex)
				newCommitIndex := matchIndex[len(matchIndex)/2]

				var term int
				if newCommitIndex > 0 {
					term = rf.log[newCommitIndex-1].CurrentTerm
				}

				if newCommitIndex > rf.commitIndex && term == rf.currentTerm {
					// fmt.Printf("Server %v ComitIndex update. %v To %v\n", rf.me, rf.commitIndex, newCommitIndex)
					rf.commitIndex = newCommitIndex
				}

				if len(isOutOfDateCh) > 0 {
					isOutOfDate = true
				}
				rf.mu.Unlock()

			}()

			time.Sleep(heartBeats * time.Millisecond)
		}
	}()
}

func (rf *Raft) createCandidateThread() {
	go func() {
		rf.mu.Lock()
		rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)
		rf.currentTerm++
		// fmt.Printf("New Election %v is Candidate, Term is %v, State is %v\n", rf.me, rf.currentTerm, rf.state)
		rf.votedFor = rf.me

		// if rf.me == 4 {
		// fmt.Printf("I'm %v, State is %v, Term is %v logs is %v\n",
		// 	rf.me, rf.state, rf.currentTerm, rf.log)
		// }

		var lastLogIndex, lastLogTerm int = 0, 0

		if len(rf.log) > 0 {
			lastLogIndex = rf.log[len(rf.log)-1].Index
			lastLogTerm = rf.log[len(rf.log)-1].CurrentTerm
		}

		serverNum := len(rf.peers)
		votes := make(chan bool, serverNum)
		votes <- true

		votedNum := make(chan bool, serverNum)

		isOutOfDate := make(chan bool)

		for i := 0; i < serverNum; i++ {
			if i != rf.me {
				go func(i int) {

					rf.mu.Lock()
					// fmt.Printf("SendRequestVote from %v to %v, CurrentTerm is %v\n", rf.me, i, rf.currentTerm)
					args := &RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					args.LastLogIndex = lastLogIndex
					args.LastLogTerm = lastLogTerm
					reply := &RequestVoteReply{}
					rf.mu.Unlock()

					ok := rf.sendRequestVote(i, args, reply)

					// fmt.Printf("Request vote to %v result : %v\n", i, ok)

					if len(isOutOfDate) > 0 {
						return
					}

					rf.mu.Lock()

					if reply.Term > rf.currentTerm {

						rf.currentTerm = reply.Term
						if rf.state != follower {
							rf.state = follower
							rf.mu.Unlock()
							rf.createFollowerThread()
							isOutOfDate <- true

							return
						}
					}

					rf.mu.Unlock()

					votedNum <- true
					if ok && reply.VoteGranted {
						votes <- true
					}
				}(i)
			}
		}

		rf.lastTime = time.Now()
		rf.mu.Unlock()

		for {
			rf.mu.Lock()
			if rf.state != candidate {
				rf.mu.Unlock()
				break
			}

			now := time.Now()
			duration := now.Sub(rf.lastTime)
			if duration > rf.electionTimeout {
				rf.mu.Unlock()
				rf.createCandidateThread()

				break
			}

			if len(votes) > int(serverNum/2) {
				rf.state = leader
				rf.mu.Unlock()

				rf.createLeaderThread()

				break
			}

			rf.mu.Unlock()

			time.Sleep(heartBeats * time.Millisecond / 10)
		}
	}()
}

func (rf *Raft) createFollowerThread() {
	// fmt.Printf("%v is Follower\n", rf.me)
	go func() {
		for {
			rf.mu.Lock()
			// fmt.Printf("I'm %v, CurrentTerm is %v\n", rf.me, rf.currentTerm)
			if rf.state != follower {
				rf.mu.Unlock()
				break
			}

			duration := time.Since(rf.lastTime)

			if duration > rf.electionTimeout {
				// fmt.Printf("Timeout %v convert to Candidate\n", rf.me)
				rf.state = candidate
				rf.mu.Unlock()
				rf.createCandidateThread()

				break
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

			// if rf.me == 4 {
			// 	fmt.Printf("I'm %v, State is %v, Term is %v logs is %v\n",
			// 		rf.me, rf.state, rf.currentTerm, rf.log)
			// }
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				applyLog := rf.log[rf.lastApplied-1]
				// fmt.Printf("I'm %v, State is %v, Index is %v, Term is %v, Command is %v, "+
				// 	"CommitIndex is %v, LastApply is %v, logs is %v\n",
				// 	rf.me, rf.state, applyLog.Index, rf.currentTerm, applyLog.Command, rf.commitIndex, rf.lastApplied, rf.log)

				go func() {
					rf.applyCh <- ApplyMsg{applyLog.Index, applyLog.Command, false, []byte{}}
				}()
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

	// rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.createFollowerThread()
	rf.createApplyThread()

	return rf
}
