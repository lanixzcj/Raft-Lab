package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import mathRand "math/rand"
import (
	"strconv"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = mathRand.Int() % len(servers)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	value := ""
	randInt := nrand()
	unix := time.Now().Unix()
	unixStr := strconv.FormatInt(unix, 10)
	unixStr = unixStr[len(unixStr)-5:]
	uuid := unixStr + "#" + strconv.FormatInt(randInt, 10)
	DPrintf(0, "Start Get: %v.\n", key)

	for {
		args := &GetArgs{key, uuid}
		reply := &GetReply{}
		ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", args, reply)

		if ok && !reply.WrongLeader {
			value = reply.Value

			DPrintf(0, "Success to get value: %v\n", value)
			return value
		}

		if reply.LeaderId != -1 {
			// client Randomize server handles.The id is ismatch
			// ck.lastLeader = reply.LeaderId
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		} else {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		}

		DPrintf(0, "ChangeLeaderId: %v\n", ck.lastLeader)

		time.Sleep(time.Millisecond * 10)
	}

	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	randInt := nrand()
	unix := time.Now().Unix()
	unixStr := strconv.FormatInt(unix, 10)
	unixStr = unixStr[len(unixStr)-5:]
	uuid := unixStr + "#" + strconv.FormatInt(randInt, 10)
	for {
		args := &PutAppendArgs{key, value, op, uuid}
		reply := &PutAppendReply{}
		ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply)
		DPrintf(-2, "%v Start Put args : %v, reply: %v.\n", ck.lastLeader, args, reply)

		if !ok {
			DPrintf(0, "Bad net: %v.\n", ck.lastLeader)
		}
		if ok && !reply.WrongLeader {
			DPrintf(0, "Success to put value: %v\n", value)
			return
		}

		if reply.LeaderId != -1 {
			// ck.lastLeader = reply.LeaderId
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		} else {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		}
		DPrintf(-2, "ChangeLeaderId: %v\n", ck.lastLeader)

		time.Sleep(time.Millisecond * 10)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
