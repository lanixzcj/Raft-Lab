package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = -3

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug > level {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string // "Put" or "Append" or "Get"
	Key   string
	Value string
	UUID  string
}

type Reply struct {
	Value string
	Index int
	Op    string
	Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValue       map[string]string
	logs           map[string]Reply
	lastIndex      int
	lastTerm       int
	lastSavedIndex int

	quitCh chan bool
}

func (kv *RaftKV) saveSnapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.lastIndex)
	e.Encode(kv.lastTerm)
	e.Encode(kv.keyValue)
	e.Encode(kv.logs)
	data := w.Bytes()
	kv.persister.SaveSnapshot(data)
}

//
// restore previously persisted state.
//
func (kv *RaftKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.lastIndex)
	d.Decode(&kv.lastTerm)
	d.Decode(&kv.keyValue)
	d.Decode(&kv.logs)
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf(0, "Server Start Get: %v\n", args)
	defer DPrintf(-1, "Server End Get: %v\n", args)
	kv.mu.Lock()
	logs, isDulpicate := kv.logs[args.UUID]
	kv.mu.Unlock()
	if isDulpicate {
		DPrintf(-1, "Dulplicate: %v, reply is %v\n", args, logs)
		reply.WrongLeader = false
		reply.Value = logs.Value
		return
	}

	starti, _, ok, leaderId := kv.rf.Start(Op{"Get", args.Key, "", args.UUID})
	term, _ := kv.rf.GetState()
	DPrintf(-1, "After Server Start Get: %v\n", ok)

	if !ok {
		reply.WrongLeader = true
		reply.LeaderId = leaderId
		reply.Err = "It's wrong leader!"
		DPrintf(-1, "Wrong leader Get: %v\n", args)
		return
	}

	now := time.Now()
	for time.Since(now).Seconds() < 1 {
		kv.mu.Lock()
		if nowTerm, isLeader := kv.rf.GetState(); !isLeader || nowTerm != term {
			reply.WrongLeader = true
			reply.LeaderId = -1
			reply.Err = "The server lost its leadership!"

			kv.mu.Unlock()
			break
		}
		if r, isExist := kv.logs[args.UUID]; isExist && r.Index == starti {
			reply.WrongLeader = false
			reply.Value = r.Value
			DPrintf(-1, "KVserver %v Success Get: %v, value is %v, reply is %v, keyvalue is %v\n", kv.me, args, r, reply, kv.keyValue)
			kv.mu.Unlock()
			return
		} else if isExist && r.Index != starti {
			reply.WrongLeader = true
			reply.LeaderId = -1
			reply.Err = "The server lost its leadership!"

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}
	DPrintf(-1, "Timeout Get: %v\n", args)
	reply.WrongLeader = true
	reply.LeaderId = -1
	reply.Err = "Timeout!"
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf(0, "Server Start Put: %v\n", args)
	defer DPrintf(-1, "Server End Put: %v\n", args)
	kv.mu.Lock()
	_, isDulpicate := kv.logs[args.UUID]
	kv.mu.Unlock()

	if isDulpicate {
		reply.WrongLeader = false
		return
	}

	starti, _, ok, leaderId := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.UUID})
	term, _ := kv.rf.GetState()
	DPrintf(-2, "After Server Start Put: %v. I'm %v, leaderId is %v\n", ok, kv.me, leaderId)
	if !ok {
		reply.WrongLeader = true
		reply.LeaderId = leaderId
		reply.Err = "It's wrong leader!"
		DPrintf(0, "Wrong leader put: %v\n", args)
		return
	}
	// replyCh := kv.replyCh

	// replyMsg := <-replyCh

	// DPrintf(0, "Reply\n")
	// DPrintf(0, "%v ==? %v\n", starti, replyMsg.Index)
	// if replyMsg.Index == starti {
	// 	reply.WrongLeader = false
	// }

	now := time.Now()
	for time.Since(now).Seconds() < 1 {
		kv.mu.Lock()
		if nowTerm, isLeader := kv.rf.GetState(); !isLeader || nowTerm != term {
			reply.WrongLeader = true
			reply.LeaderId = -1
			reply.Err = "The server lost its leadership!"

			kv.mu.Unlock()
			break
		}
		if r, isExist := kv.logs[args.UUID]; isExist && r.Index == starti {
			DPrintf(-1, "KVserver %v Success put: %v, keyvalue is %v\n", kv.me, args, kv.keyValue)
			kv.mu.Unlock()
			reply.WrongLeader = false

			return
		} else if isExist && r.Index != starti {
			reply.WrongLeader = true
			reply.LeaderId = -1
			reply.Err = "The server lost its leadership!"

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}

	DPrintf(-1, "TImeout: %v\n", args)
	reply.WrongLeader = true
	reply.LeaderId = -1
	// reply.Err = " Timeout!"
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.mu.Lock()
	close(kv.quitCh)
	DPrintf(-3, "Kill kvserver %v\n", kv.me)
	kv.mu.Unlock()
	kv.rf.Kill()

	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.lastSavedIndex = -1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.quitCh = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.keyValue = make(map[string]string)
	kv.logs = make(map[string]Reply)

	kv.readSnapshot(persister.ReadSnapshot())
	applyCh := kv.applyCh
	// replyCh := kv.replyCh
	go func() {
		for m := range applyCh {
			kv.mu.Lock()
			select {
			case <-kv.quitCh:
				kv.mu.Unlock()
				return
			default:
				me := kv.me

				DPrintf(-3, "%v Apply MSg: %v\n", me, m)

				if m.UseSnapshot {
					DPrintf(0, "%v Save snapshot\n", me)
					DPrintf(0, "%v Before keyvalue: %v\n", me, kv.keyValue)
					kv.persister.SaveSnapshot(m.Snapshot)
					kv.readSnapshot(kv.persister.ReadSnapshot())
					DPrintf(0, "%v After keyvalue: %v\n", me, kv.keyValue)
					kv.lastSavedIndex = -1
					kv.mu.Unlock()
					continue
				}
				reply := Reply{}
				reply.Index = m.Index

				op := m.Command.(Op)

				kv.lastIndex = m.Index
				kv.lastTerm = m.Term
				DPrintf(0, "kv server %v update lastIndex: %v, last term: %v\n", kv.me, m.Index, m.Term)

				_, ok := kv.logs[op.UUID]

				if ok {
					DPrintf(0, "%v's duplicate %v, logs: %v\n", me, m, kv.logs)

					kv.mu.Unlock()
					continue
				}

				reply.Op = op.Op
				if op.Op == "Put" {
					kv.keyValue[op.Key] = op.Value
				} else if op.Op == "Append" {
					kv.keyValue[op.Key] = kv.keyValue[op.Key] + op.Value
				} else {
					reply.Value = kv.keyValue[op.Key]
				}

				kv.logs[op.UUID] = reply

				DPrintf(-3, "%v's keyValue: %v\n", me, kv.keyValue)
				DPrintf(-3, "%v's logs: %v\n", me, kv.logs)
				DPrintf(-3, "Finish %v Apply MSg: %v\n", me, m)
			}

			kv.mu.Unlock()
		}
	}()

	if kv.maxraftstate != -1 {
		go func() {
			for {
				kv.mu.Lock()
				select {
				case <-kv.quitCh:
					kv.mu.Unlock()
					return
				default:

					if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate &&
						kv.lastSavedIndex != kv.lastIndex {
						kv.saveSnapshot()
						kv.rf.SetDiscardIndex(kv.lastIndex, kv.lastTerm)
						kv.lastSavedIndex = kv.lastIndex
						DPrintf(-3, "Server %v save snapshot lastIndex: %v lastTerm: %v\n", me, kv.lastIndex, kv.lastTerm)
					} else {

					}
				}

				kv.mu.Unlock()
				time.Sleep(time.Millisecond * 50)
			}
		}()
	}

	// You may need initialization code here.

	return kv
}
