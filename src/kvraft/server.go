package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = -2

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
	Uuid  int64
}

type Reply struct {
	Value string
	Index int
	Err
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValue map[string]string
	logs     map[int64]Reply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf(0, "Server Start Get: %v\n", args)
	defer DPrintf(-1, "Server End Get: %v\n", args)
	kv.mu.Lock()
	logs, isDulpicate := kv.logs[args.Uuid]
	kv.mu.Unlock()
	if isDulpicate {
		DPrintf(-1, "Dulplicate: %v\n", args)
		reply.WrongLeader = false
		reply.Value = logs.Value
		return
	}

	starti, _, ok := kv.rf.Start(Op{"Get", args.Key, "", args.Uuid})
	// term, _ := kv.rf.GetState()
	DPrintf(-1, "After Server Start Get: %v\n", ok)

	if !ok {
		reply.WrongLeader = true
		reply.Err = "It's wrong leader!"
		DPrintf(-1, "Wrong leader Get: %v\n", args)
		return
	}

	// replyCh := kv.replyCh

	// replyMsg := <-replyCh

	// if replyMsg.Index == starti {
	// 	reply.WrongLeader = false
	// 	reply.Value = replyMsg.Value
	// }

	now := time.Now()
	for time.Since(now).Seconds() < 2 {
		kv.mu.Lock()
		// if nowTerm, isLeader := kv.rf.GetState(); !isLeader || nowTerm != term {
		// 	reply.WrongLeader = true
		// 	reply.Err = "The server lost its leadership!"

		// 	kv.mu.Unlock()
		// 	break
		// }
		if r, isExist := kv.logs[args.Uuid]; isExist && r.Index == starti {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Value = r.Value
			DPrintf(-1, "Success Get: %v\n", args)
			return
		} else if isExist && r.Index != starti {
			reply.WrongLeader = true
			reply.Err = "The server lost its leadership!"

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}
	DPrintf(-1, "Timeout Get: %v\n", args)
	reply.WrongLeader = true
	reply.Err = "Timeout!"
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf(0, "Server Start Put: %v\n", args)
	defer DPrintf(-1, "Server End Put: %v\n", args)
	kv.mu.Lock()
	_, isDulpicate := kv.logs[args.Uuid]
	kv.mu.Unlock()

	if isDulpicate {
		reply.WrongLeader = false
		return
	}

	starti, _, ok := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.Uuid})
	// term, _ := kv.rf.GetState()
	DPrintf(-2, "After Server Start Put: %v\n", ok)
	if !ok {
		reply.WrongLeader = true
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
	for time.Since(now).Seconds() < 3 {
		kv.mu.Lock()
		// if nowTerm, isLeader := kv.rf.GetState(); !isLeader || nowTerm != term {
		// 	reply.WrongLeader = true
		// 	reply.Err = "The server lost its leadership!"

		// 	kv.mu.Unlock()
		// 	break
		// }
		if r, isExist := kv.logs[args.Uuid]; isExist && r.Index == starti {
			kv.mu.Unlock()
			reply.WrongLeader = false
			DPrintf(-2, "Success put: %v\n", args)
			return
		} else if isExist && r.Index != starti {
			reply.WrongLeader = true
			reply.Err = "The server lost its leadership!"

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		time.Sleep(time.Millisecond * 10)
	}

	DPrintf(-1, "TImeout: %v\n", args)
	reply.WrongLeader = true
	// reply.Err = " Timeout!"
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.keyValue = make(map[string]string)
	kv.logs = make(map[int64]Reply)

	applyCh := kv.applyCh
	// replyCh := kv.replyCh
	go func() {
		for m := range applyCh {
			kv.mu.Lock()

			me := kv.me

			DPrintf(0, "%v Apply MSg: %v\n", me, m)
			reply := Reply{}
			reply.Index = m.Index

			op := m.Command.(Op)

			_, ok := kv.logs[op.Uuid]

			if ok {
				DPrintf(0, "%v's duplicate %v, logs: %v\n", me, m, kv.logs)

				kv.mu.Unlock()
				continue
			}
			if op.Op == "Put" {
				kv.keyValue[op.Key] = op.Value
			} else if op.Op == "Append" {
				kv.keyValue[op.Key] = kv.keyValue[op.Key] + op.Value
			} else {
				reply.Value = kv.keyValue[op.Key]
			}

			kv.logs[op.Uuid] = reply

			DPrintf(0, "%v's keyValue: %v\n", me, kv.keyValue)
			DPrintf(0, "%v's logs: %v\n", me, kv.logs)
			DPrintf(-2, "Finish %v Apply MSg: %v\n", me, m)
			kv.mu.Unlock()
		}
	}()

	// You may need initialization code here.

	return kv
}
