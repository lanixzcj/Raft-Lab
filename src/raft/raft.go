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
	Term        int
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
const chunkSize = 1000

type LogEntry struct {
	Index       int
	CurrentTerm int
	Command     interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	leaderId           int
	discardIndex       int
	snapshotFromLeader []byte
	tempSnapshot       []byte

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

	nextIndex            []int
	matchIndex           []int
	isInstallingSnapshot []bool

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

func readSnapshot(data []byte) (int, int) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return 0, 0
	}

	var lastIndex, lastTerm int
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIndex)
	d.Decode(&lastTerm)

	return lastIndex, lastTerm
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
// AppendEntries RPC reply strcture
//
type AppendEntriesReply struct {
	Term          int
	Success       bool
	SuitableTerm  int
	SuitableIndex int
}

//
// InstallSnapshot RPC reply strcture
//
type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Offset           int
	Data             []byte
	Done             bool
}

//
// InstallSnapshot RPC reply strcture
//
type InstallSnapshotReply struct {
	Term int
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
			go rf.createFollowerThread()
		}
	}

	var lastLogIndex, lastLogTerm int = 0, 0

	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].CurrentTerm
	} else {
		lastLogIndex, lastLogTerm = readSnapshot(rf.persister.ReadSnapshot())
	}

	isUpToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		rf.votedFor = args.CandidateId
		DPrintf(1, "RequestVote :%v voted %v, log is : %v\n", rf.me, rf.votedFor, rf.log)
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
		// fmt.Printf("%v request failed, I have already voted %v\n", args.CandidateId, rf.votedFor)
	}

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf(1, "Append HeartBeats from %v, args :%v, , I'm %v, CurrentTerm is %v, log is %v\n",
		args.LeaderId, args, rf.me, rf.currentTerm, rf.log)
	rf.lastTime = time.Now()
	rf.leaderId = args.LeaderId

	reply.Term = rf.currentTerm

	logLength := len(rf.log)

	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false

		return
	} else if args.Term >= rf.currentTerm {
		if rf.votedFor != -1 {
			DPrintf(1, "AppendRPC out of date, %v to %v\n", rf.currentTerm, args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		if rf.state != follower {
			rf.state = follower
			go rf.createFollowerThread()
		}
	}

	index := logLength

	if index > 0 {
		DPrintf(0, "1.I'm %v, index: %v term: %v; log : %v, args :%v", rf.me, reply.SuitableIndex, reply.SuitableTerm, rf.log, args)
		// if args.PrevLogIndex > rf.log[index-1].Index {
		// 	reply.Success = false
		// 	reply.SuitableTerm = rf.log[index-1].CurrentTerm
		// 	reply.SuitableIndex = rf.log[index-1].Index
		// }

		DPrintf(0, "2.I'm %v,  index: %v term: %v; log : %v, args :%v", rf.me, reply.SuitableIndex, reply.SuitableTerm, rf.log, args)

		for i := logLength - 1; i >= 0; i-- {
			if args.PrevLogIndex == rf.log[i].Index && args.PrevLogTerm == rf.log[i].CurrentTerm {
				index = i

				break
			}

			// if args.PrevLogIndex == rf.log[i].Index && args.PrevLogTerm > rf.log[i].CurrentTerm {
			// 	reply.Success = false
			// 	reply.SuitableTerm = rf.log[i].CurrentTerm
			// }

			// if args.PrevLogIndex > rf.log[i].Index && reply.SuitableTerm == rf.log[i].CurrentTerm {
			// 	reply.SuitableIndex = rf.log[i].Index
			// } else if args.PrevLogIndex > rf.log[i].Index && reply.SuitableTerm < rf.log[i].CurrentTerm {
			// 	DPrintf(1, "Append args: %v, I'm %v, SuitableIndex is %v, SuitableTerm is %v, success is %v, CommitIndex is %v\n",
			// 		args, rf.me, reply.SuitableIndex, reply.SuitableTerm, reply.Success, rf.commitIndex)
			// 	return
			// }
			if args.PrevLogIndex > rf.log[i].Index && args.PrevLogTerm > rf.log[i].CurrentTerm {
				reply.Success = false
				reply.SuitableTerm = rf.log[i].CurrentTerm
				reply.SuitableIndex = rf.log[i].Index

				DPrintf(1, "Append args: %v, I'm %v, SuitableIndex is %v, SuitableTerm is %v, success is %v, CommitIndex is %v\n",
					args, rf.me, reply.SuitableIndex, reply.SuitableTerm, reply.Success, rf.commitIndex)

				return
			}
		}

		DPrintf(0, "3.I'm %v,  index: %v term: %v; log : %v, args :%v", rf.me, reply.SuitableIndex, reply.SuitableTerm, rf.log, args)

		// if reply.SuitableTerm > 0 && reply.SuitableIndex > 0 {
		// 	DPrintf(1, "Append args: %v, I'm %v, SuitableIndex is %v, SuitableTerm is %v, success is %v, CommitIndex is %v\n",
		// 		args, rf.me, reply.SuitableIndex, reply.SuitableTerm, reply.Success, rf.commitIndex)
		// 	return
		// }
	}

	if index == logLength {
		lastIndex, lastTerm := readSnapshot(rf.persister.ReadSnapshot())
		DPrintf(1, "Append args: %v, I'm %v, lastIndex is %v, lastTerm is %v, from snapshot\n",
			args, rf.me, lastIndex, lastTerm)

		if args.PrevLogIndex == lastIndex && args.PrevLogTerm == lastTerm {
			index = -1
		} else if args.PrevLogIndex > lastIndex && args.PrevLogTerm >= lastTerm {
			reply.Success = false
			reply.SuitableIndex = lastIndex
			reply.SuitableTerm = lastTerm

			DPrintf(1, "Append args: %v, I'm %v, SuitableIndex is %v, SuitableTerm is %v, success is %v, CommitIndex is %v\n",
				args, rf.me, reply.SuitableIndex, reply.SuitableTerm, reply.Success, rf.commitIndex)
			return
		} else {
			reply.Success = false
			reply.SuitableIndex = -1
			reply.SuitableTerm = -1

			DPrintf(1, "Snapshot out of date.Append args: %v, I'm %v, SuitableIndex is %v, SuitableTerm is %v, success is %v, CommitIndex is %v\n",
				args, rf.me, reply.SuitableIndex, reply.SuitableTerm, reply.Success, rf.commitIndex)
			return
		}
	}

	reply.Success = true
	DPrintf(0, "4.I'm %v,  index: %v term: %v; log : %v, args :%v", rf.me, reply.SuitableIndex, reply.SuitableTerm, rf.log, args)

	DPrintf(1, "Server %v(term:%v) AppendEntry from %v(term:%v), before : %v, startIndex: %v, appendEntries:%v\n",
		rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log, index, args.Entries)

	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:index+1], args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex && len(rf.log) > 0 {
		lastIndex := math.MaxInt32
		lastIndex = rf.log[len(rf.log)-1].Index

		rf.commitIndex = Min(args.LeaderCommit, lastIndex)
	}

	DPrintf(1, "Append args: %v, I'm %v, after append logs:%v, SuitableIndex is %v, SuitableTerm is %v, success is %v, CommitIndex is %v\n",
		args, rf.me, rf.log, reply.SuitableIndex, reply.SuitableTerm, reply.Success, rf.commitIndex)
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.lastTime = time.Now()
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || len(rf.snapshotFromLeader) > 0 {
		return
	}

	rf.currentTerm = args.Term

	if args.Offset == 0 {
		rf.tempSnapshot = make([]byte, 0)
	}

	if args.Offset != len(rf.tempSnapshot) {
		DPrintf(2, "Server %v Start to install snapshot. offset %v is not matched \n",
			rf.me, args.Offset, len(rf.tempSnapshot))
		return
	}

	DPrintf(2, "Server %v Start to install snapshot. args is %v. snapshotSize is %v. Log is %v . \n",
		rf.me, args, len(rf.tempSnapshot), rf.log)
	rf.tempSnapshot = append(rf.tempSnapshot[:args.Offset], args.Data...)

	if args.Done {
		rf.snapshotFromLeader = rf.tempSnapshot
		rf.tempSnapshot = nil
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool, int) {
	index := -1
	term := -1
	isLeader := true
	lastLeader := -1

	// Your code here (2B).

	rf.mu.Lock()
	isLeader = rf.state == leader
	lastLeader = rf.leaderId

	if isLeader {
		if len(rf.log) > 0 {
			index = rf.log[len(rf.log)-1].Index + 1
		} else {
			index, _ = readSnapshot(rf.persister.ReadSnapshot())
			index++
		}
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
		rf.persist()
		DPrintf(-1, "Leader %v Start a command %v, term is %v, index is %v\n", rf.me, command, term, index)
	}

	rf.mu.Unlock()

	return index, term, isLeader, lastLeader
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
	rf.leaderId = -1

	logLength := len(rf.log)
	var lastIndex int
	if logLength > 0 {
		lastIndex = rf.log[logLength-1].Index
	} else {
		lastIndex = 0
	}

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastIndex + 1
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

				if rf.state != leader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				DPrintf(2, "Leader %v, Term %v Goroutine Num: %v, me is %v, CommitIndex is %v, ApplyIndex is %v, log is %v, "+
					"matchIndex:%v, nextIndex:%v\n",
					rf.me, rf.currentTerm, runtime.NumGoroutine(), GoID(), rf.commitIndex, rf.lastApplied, rf.log, rf.matchIndex, rf.nextIndex)

				serverNum := len(rf.peers)

				for i := 0; i < serverNum; i++ {
					if i != rf.me {
						go func(i int) {

							rf.mu.Lock()

							if rf.state != leader {
								rf.mu.Unlock()

								return
							}

							if rf.nextIndex[i] < 0 {
								if rf.isInstallingSnapshot[i] {
									rf.mu.Unlock()

									return
								}
								rf.isInstallingSnapshot[i] = true

								snapshotArgs := &InstallSnapshotArgs{}
								snapshotArgs.Term = rf.currentTerm
								snapshotArgs.LeaderId = rf.me
								snapshotArgs.Offset = 0

								snapshotArgs.LastIncludeIndex, snapshotArgs.LastIncludeTerm =
									readSnapshot(rf.persister.ReadSnapshot())
								data := make([]byte, len(rf.persister.ReadSnapshot()))
								copy(data, rf.persister.ReadSnapshot())
								snapshotReply := &InstallSnapshotReply{}

								offset := 0

								for len(data) > 0 {
									snapshotArgs.Offset = offset
									if len(data) > chunkSize {
										snapshotArgs.Done = false
										snapshotArgs.Data = data[:chunkSize]
										data = data[chunkSize:]
										offset += chunkSize
									} else {
										snapshotArgs.Done = true
										snapshotArgs.Data = data[:]
										data = nil
									}

									rf.mu.Unlock()
									DPrintf(2, "Leader %v install snapshot chunks to %v, offset is %v, data is %v\n",
										rf.me, i, snapshotArgs.Offset, snapshotArgs.Data)

									var ok = false
									now := time.Now()
									for !ok && time.Since(now).Seconds() < 3 {
										ok = rf.sendInstallSnapshot(i, snapshotArgs, snapshotReply)

										if !ok {
											DPrintf(2, "Leader %v install snapshot chunks to %v failed. Re-try it.\n",
												rf.me, i)
										}
									}

									rf.mu.Lock()

									if !ok {
										DPrintf(2, "Leader %v install snapshot to %v failed.\n", rf.me, i)
										rf.isInstallingSnapshot[i] = false
										rf.mu.Unlock()
										return
									}
									if snapshotReply.Term > rf.currentTerm {
										DPrintf(2, "Leader %v back to Follower，from term %v to %v\n", rf.me, rf.currentTerm, snapshotReply.Term)
										rf.isInstallingSnapshot[i] = false
										rf.currentTerm = snapshotReply.Term
										rf.votedFor = -1
										defer rf.mu.Unlock()
										defer rf.persist()
										if rf.state != follower {
											rf.state = follower
											rf.lastTime = time.Now()
											go rf.createFollowerThread()
										}
										return
									}
								}
								logLength := len(rf.log)
								var lastIndex int
								if logLength > 0 {
									lastIndex = rf.log[logLength-1].Index
								} else {
									lastIndex = 0
								}
								rf.nextIndex[i] = lastIndex + 1
								rf.isInstallingSnapshot[i] = false
								DPrintf(2, "Leader %v finish install snapshot to %v.\n", rf.me, i)
							}
							args := &AppendEntriesArgs{}
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.LeaderCommit = rf.commitIndex

							logLength := len(rf.log)
							var lastIndex int
							if logLength > 0 {
								lastIndex = rf.log[logLength-1].Index
							} else {
								lastIndex = 0
							}
							firstIndex := 0
							if lastIndex > 0 {
								firstIndex = rf.log[0].Index
							}

							DPrintf(0, "%v, PreAppendEntry, fistIndex: %v, lastIndex: %v, nextIndex is %v, log is %v\n",
								rf.me, firstIndex, lastIndex, rf.nextIndex, rf.log)
							prevIndex := rf.nextIndex[i] - firstIndex - 1
							if rf.nextIndex[i] > 1 && len(rf.log) > 0 &&
								(prevIndex >= 0 && prevIndex <= len(rf.log)-1) {
								args.PrevLogIndex = rf.log[prevIndex].Index
								args.PrevLogTerm = rf.log[prevIndex].CurrentTerm
								args.Entries = rf.log[prevIndex+1:]
							} else {
								args.PrevLogIndex, args.PrevLogTerm =
									readSnapshot(rf.persister.ReadSnapshot())

								args.Entries = []LogEntry{}
								if logLength > 0 {
									args.Entries = rf.log[:]
								}
							}

							reply := &AppendEntriesReply{}

							rf.mu.Unlock()

							now := time.Now()
							ok := rf.sendAppendEntries(i, args, reply)

							rf.mu.Lock()
							defer rf.mu.Unlock()

							DPrintf(2, "HeartBeats from %v to %v result : %v, match: %v, spent %v s\n",
								rf.me, i, ok, rf.matchIndex, time.Since(now).Seconds())

							if ok && reply.Success {
								rf.nextIndex[i] = lastIndex + 1
								rf.matchIndex[i] = lastIndex
								DPrintf(2, "HeartBeats from %v to %v result : %v, match: %v\n",
									rf.me, i, ok, rf.matchIndex)
							} else if ok && !reply.Success {
								if reply.Term > rf.currentTerm {
									DPrintf(2, "Leader %v back to Follower，from term %v to %v\n", rf.me, rf.currentTerm, reply.Term)

									rf.currentTerm = reply.Term
									rf.votedFor = -1
									defer rf.persist()
									if rf.state != follower {
										rf.state = follower
										rf.lastTime = time.Now()
										go rf.createFollowerThread()
									}

									return

								}

								if reply.SuitableIndex == -1 && reply.SuitableTerm == -1 {
									DPrintf(2, "Follower %v lag beind, need install snapshot\n", i)
									rf.nextIndex[i] = -1
								} else {
									firstIndex := rf.nextIndex[i]
									if lastIndex > 0 {
										firstIndex = rf.log[0].Index
									}
									for j := rf.nextIndex[i] - firstIndex; j > 0; j-- {
										if (rf.log[j-1].Index == reply.SuitableIndex && rf.log[j-1].CurrentTerm == reply.SuitableTerm) ||
											(rf.log[j-1].CurrentTerm < reply.SuitableTerm && rf.log[j-1].Index < reply.SuitableIndex) {
											rf.nextIndex[i] = j + firstIndex

											return
										}
									}
									rf.nextIndex[i] = 0

									lastIncludeIndex, lastIncludeTerm := readSnapshot(rf.persister.ReadSnapshot())

									if lastIncludeIndex > reply.SuitableIndex || lastIncludeTerm > reply.SuitableTerm {
										DPrintf(2, "Follower %v lag beind, need install snapshot\n", i)
										rf.nextIndex[i] = -1
									}
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
				if newCommitIndex > rf.commitIndex && len(rf.log) > 0 {
					firstIndex := rf.log[0].Index
					term = rf.log[newCommitIndex-firstIndex].CurrentTerm
				}

				if newCommitIndex > rf.commitIndex && term == rf.currentTerm {
					DPrintf(2, "Server %v ComitIndex update. %v To %v, matchIndex:%v\n",
						rf.me, rf.commitIndex, newCommitIndex, rf.matchIndex)
					rf.commitIndex = newCommitIndex
				} else if newCommitIndex > rf.commitIndex {
					DPrintf(0, "The commited entry should be created by this leader %v, log is %v, term is %v, newCommitIndex is %v\n",
						rf.me, rf.log, rf.currentTerm, newCommitIndex)
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
		rf.lastTime = time.Now()
		rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)
		rf.leaderId = -1

		rf.currentTerm++
		DPrintf(0, "New Election: %v is Candidate,  goroutine is %v,  Term is %v, State is %v, log is %v\n",
			rf.me, GoID(), rf.currentTerm, rf.state, rf.log)
		rf.votedFor = rf.me
		rf.persist()

		var lastLogIndex, lastLogTerm int = 0, 0

		if len(rf.log) > 0 {
			lastLogIndex = rf.log[len(rf.log)-1].Index
			lastLogTerm = rf.log[len(rf.log)-1].CurrentTerm
		} else {
			lastLogIndex, lastLogTerm = readSnapshot(rf.persister.ReadSnapshot())
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
							rf.lastTime = time.Now()
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
	rf.mu.Lock()
	rf.leaderId = -1
	rf.mu.Unlock()
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
				if len(rf.snapshotFromLeader) > 0 {
					data := make([]byte, len(rf.snapshotFromLeader))
					copy(data, rf.snapshotFromLeader)
					DPrintf(0, "Need apply snapshot to the state machine :%v", data)

					rf.mu.Unlock()
					rf.applyCh <- ApplyMsg{0, 0, nil, true, data}
					rf.mu.Lock()

					lastIndex, lastTerm := readSnapshot(data)
					index := 0
					for i, logEntry := range rf.log {
						if logEntry.Index == lastIndex &&
							logEntry.CurrentTerm == lastTerm {
							index = i
							break
						}

						index++
					}

					if index == len(rf.log) {
						rf.log = make([]LogEntry, 0)
					} else {
						remainLogs := make([]LogEntry, len(rf.log[index+1:]))
						copy(remainLogs, rf.log[index+1:])
						rf.log = remainLogs
					}

					rf.commitIndex = lastIndex
					rf.lastApplied = lastIndex

					DPrintf(2, "Server %v End to install snapshot. Log is %v . lastIndex is %v, lastTerm is %v\n",
						rf.me, rf.log, lastIndex, lastTerm)
					rf.snapshotFromLeader = nil
				}
				for rf.commitIndex > rf.lastApplied {
					rf.lastApplied++

					logLength := len(rf.log)

					firstIndex := 0
					if logLength > 0 {
						firstIndex = rf.log[0].Index
					}
					index := rf.lastApplied - firstIndex
					if index < 0 || logLength == 0 {
						continue
					}
					applyLog := rf.log[rf.lastApplied-firstIndex]

					DPrintf(0, "Apply command to the state machine.I'm %v, State is %v, Index is %v, Term is %v,"+
						"Command is %v, CommitIndex is %v, LastApply is %v, logs is %v,firstIndex, %v\n",
						rf.me, rf.state, applyLog.Index, rf.currentTerm, applyLog.Command, rf.commitIndex, rf.lastApplied, rf.log, firstIndex)

					rf.mu.Unlock()

					rf.applyCh <- ApplyMsg{applyLog.Index, applyLog.CurrentTerm, applyLog.Command, false, []byte{}}

					rf.mu.Lock()
				}
			}
			rf.mu.Unlock()

			time.Sleep(heartBeats * time.Millisecond / 10)
		}
	}()
}

func (rf *Raft) createDiscardThread() {
	go func() {
		for {
			rf.mu.Lock()
			select {
			case <-rf.quitCh:
				rf.mu.Unlock()
				return
			default:
				if rf.discardIndex > 0 {
					index := 0

					for i, logEntry := range rf.log {
						if logEntry.Index == rf.discardIndex {
							index = i + 1
							break
						}

						if logEntry.Index > rf.discardIndex {
							break
						}
					}

					DPrintf(-2, "Server %v discard logs.discardIndex is %v, index is %v, logs is :%v\n",
						rf.me, rf.discardIndex, index, rf.log)

					if index > -1 && index <= rf.lastApplied {
						remainLogs := make([]LogEntry, len(rf.log[index:]))
						copy(remainLogs, rf.log[index:])
						rf.log = remainLogs

						rf.persist()
					}
					DPrintf(-2, "Server %v after discard logs.discardIndex is %v, index is %v, logs is :%v\n",
						rf.me, rf.discardIndex, index, rf.log)

					rf.discardIndex = 0
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
	rf.leaderId = -1
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.isInstallingSnapshot = make([]bool, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	rf.lastTime = time.Now()

	rf.quitCh = make(chan bool)

	rf.electionTimeout = time.Millisecond * time.Duration(rand.Intn(heartBeats+50)+heartBeats+50)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.log) > 0 {
		rf.commitIndex = rf.log[0].Index - 1
		rf.lastApplied = rf.commitIndex - 1
	} else {
		rf.commitIndex, rf.lastApplied = readSnapshot(rf.persister.ReadSnapshot())
	}

	rf.createFollowerThread()
	rf.createApplyThread()
	rf.createDiscardThread()

	return rf
}

func (rf *Raft) SetDiscardIndex(discardIndex, term int) {
	rf.mu.Lock()
	DPrintf(-2, "Server %v. Set discard logs, index: %v\n",
		rf.me, discardIndex)
	rf.discardIndex = discardIndex
	// rf.lastIncludeTerm = term
	rf.mu.Unlock()
}
