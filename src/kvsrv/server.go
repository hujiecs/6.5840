package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type CacheItem struct {
	opId int64
	key  string
	val  string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	myMap map[string]string

	cacheResult map[int64]*CacheItem
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Printf("Get from %d, key: %v\n", args.ClerkId, args.Key)
	if val, ok := kv.myMap[args.Key]; ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Printf("Put from %d, key: %v\n", args.ClerkId, args.Key)
	if val, ok := kv.cacheResult[args.ClerkId]; ok {
		if val.opId == args.OpId && val.key == args.Key {
			return
		}
	}

	// put doesn't return result, only record the operation
	kv.UpdateCache(args.ClerkId, args.OpId, args.Key, "")
	kv.myMap[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Printf("Append from %d, key: %v\n", args.ClerkId, args.Key)
	if val, ok := kv.cacheResult[args.ClerkId]; ok {
		if val.opId == args.OpId && val.key == args.Key {
			reply.Value = val.val
			return
		}
	}

	oldVal := kv.myMap[args.Key]
	newVal := oldVal + args.Value

	kv.UpdateCache(args.ClerkId, args.OpId, args.Key, oldVal)
	kv.myMap[args.Key] = newVal
	reply.Value = oldVal
}

// must be called with kv mu lock hold
func (kv *KVServer) UpdateCache(clerkId int64, opId int64, key string, val string) {
	kv.cacheResult[clerkId] = &CacheItem{opId: opId, key: key, val: val}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.myMap = make(map[string]string)
	kv.cacheResult = make(map[int64]*CacheItem)

	return kv
}
