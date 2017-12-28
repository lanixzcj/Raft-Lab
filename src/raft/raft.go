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
	"math/rand"
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
	state       int
	currentTerm int
	votedFor    int
	log         []interface{}

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
	prevLogTerm  int
	Entries      []interface{}
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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
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

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false

		return
	} else if args.Term >= rf.currentTerm {
		// fmt.Printf("RequestVote out of date, %v to %v\n", rf.currentTerm, args.Term)
		rf.currentTerm = args.Term

		if rf.state != follower {
			rf.state = follower
			rf.createFollowerThread()
		}
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
	go func() {
		for {
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				break
			}
			fmt.Printf("%v is Leader, Term is %v, State is %v, address %v\n", rf.me, rf.currentTerm, rf.state, &rf)

			serverNum := len(rf.peers)

			isOutOfDate := make(chan bool)

			for i := 0; i < serverNum; i++ {
				if i != rf.me {
					go func(i int) {
						rf.mu.Lock()
						// fmt.Printf("SendHeartBeats from %v to %v\n", me, i)
						// fmt.Printf("I'm %v, CurrentTerm is %v\n", rf.me, rf.currentTerm)

						args := &AppendEntriesArgs{}
						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.LeaderCommit = rf.commitIndex
						reply := &AppendEntriesReply{}
						rf.mu.Unlock()

						ok := rf.sendAppendEntries(i, args, reply)

						if ok {

						}
						// fmt.Printf("HeartBeats to %v result : %v\n", i, ok)

						if len(isOutOfDate) > 0 {
							return
						}

						rf.mu.Lock()

						// fmt.Printf("Me %v, Beats Reply Term %v ,CurrentTerm %v\n", rf.me, reply.Term, rf.currentTerm)
						if reply.Term > rf.currentTerm {
							// fmt.Printf("out Of Date\n")
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
					}(i)
				}
			}

			rf.mu.Unlock()

			if len(isOutOfDate) > 0 {
				break
			}

			time.Sleep(heartBeats * time.Millisecond)
		}
	}()
}

func (rf *Raft) createCandidateThread() {
	go func() {
		rf.mu.Lock()
		rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)
		rf.currentTerm++
		fmt.Printf("%v is Candidate, %v\n", rf.me, rf.currentTerm)
		rf.votedFor = rf.me

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

			if len(votedNum) == serverNum {
				return
			}

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

			now := time.Now()

			duration := now.Sub(rf.lastTime)

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
	rf.log = make([]interface{}, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.lastTime = time.Now()

	// rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.createFollowerThread()

	return rf
}
