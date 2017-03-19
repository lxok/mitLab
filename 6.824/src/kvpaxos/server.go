package kvpaxos

import (
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq     int
	Propser int
	Id      int64

	Operation string
	Key       string
	Value     string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	database map[string]string

	serverLog []Op
	cp        int //commit point
	ap        int //apply point
	fp        int //free memory point

	filter map[int64]int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	//make a Op
	var getOp Op
	getOp.Propser = kv.me
	getOp.Id = args.Id
	getOp.Operation = Get
	getOp.Key = args.Key
	getOp.Value = ""

	//select a seq
	status, val := kv.px.Status(kv.cp)
	for status != paxos.Pending {
		if status == paxos.Decided {
			kv.serverLog[kv.cp] = val.(Op)
		}
		kv.cp++
		status, val = kv.px.Status(kv.cp)
	}

	getOp.Seq = kv.cp

	//make a log, when change putOp' seq, must rewrite in log.
	kv.serverLog[kv.cp] = getOp
	kv.cp++

	//make a instance
	kv.px.Start(getOp.Seq, getOp)

	//get result, if be seized, start a instance again
	to := 10 * time.Millisecond
	status, val = kv.px.Status(getOp.Seq)
	for !(status == paxos.Decided && val.(Op).Id == getOp.Id) {
		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = kv.px.Status(getOp.Seq)
			continue
		}
		if val.(Op).Id != getOp.Id {
			kv.serverLog[getOp.Seq] = val.(Op)
			getOp.Seq = kv.cp
			kv.serverLog[kv.cp] = getOp
			kv.cp++

			kv.px.Start(getOp.Seq, getOp)
			status, val = kv.px.Status(getOp.Seq)
		}
	}

	//execute actual operation, reply the value
	if result := kv.ExeDatabase(getOp.Seq); result == "" {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
		reply.Value = result
	}

	//free memory
	kv.FreeMemory(getOp.Seq)

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	//make a Op
	var putOp Op
	putOp.Id = args.Id
	putOp.Propser = kv.me
	putOp.Operation = args.Op
	putOp.Key = args.Key
	putOp.Value = args.Value

	//select a seq
	status, val := kv.px.Status(kv.cp)
	for status != paxos.Pending {
		if status == paxos.Decided {
			kv.serverLog[kv.cp] = val.(Op)
		}
		kv.cp++
		status, val = kv.px.Status(kv.cp)
	}

	putOp.Seq = kv.cp

	//make a log, when change putOp' seq, must rewrite in log.
	kv.serverLog[kv.cp] = putOp
	kv.cp++

	//make a instance
	kv.px.Start(putOp.Seq, putOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val = kv.px.Status(putOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).Id == putOp.Id) {

		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = kv.px.Status(putOp.Seq)
			continue
		}
		if val.(Op).Id != putOp.Id {
			kv.serverLog[putOp.Seq] = val.(Op)
			putOp.Seq = kv.cp
			kv.serverLog[kv.cp] = putOp
			kv.cp++

			kv.px.Start(putOp.Seq, putOp)
			status, val = kv.px.Status(putOp.Seq)
		}
	}

	//execute actual operation
	kv.ExeDatabase(putOp.Seq)
	//	for k, v := range kv.database {
	//		fmt.Println("database:", kv.me, " key:", k, " value:", v)
	//	}

	//free memory
	kv.FreeMemory(putOp.Seq)

	//reply,return
	reply.Err = OK

	return nil
}

func (kv *KVPaxos) ExeDatabase(seq int) string {

	result := ""

	for kv.ap <= seq {
		if _, ok := kv.filter[kv.serverLog[kv.ap].Id]; ok {
			kv.ap++
			continue
		}

		if kv.serverLog[kv.ap].Operation == Put {
			kv.database[kv.serverLog[kv.ap].Key] = kv.serverLog[kv.ap].Value
			kv.filter[kv.serverLog[kv.ap].Id] = 1
		} else if kv.serverLog[kv.ap].Operation == Append {
			if v, ok := kv.database[kv.serverLog[kv.ap].Key]; ok {
				kv.database[kv.serverLog[kv.ap].Key] = v + kv.serverLog[kv.ap].Value
			} else {
				kv.database[kv.serverLog[kv.ap].Key] = kv.serverLog[kv.ap].Value
			}
			kv.filter[kv.serverLog[kv.ap].Id] = 1
		} else {
			//Operation = Get
		}

		if kv.ap == seq && kv.serverLog[kv.ap].Operation == Get {
			if v, ok := kv.database[kv.serverLog[kv.ap].Key]; ok {
				result = v
			}
		}

		kv.ap++
	}

	return result
}

func (kv *KVPaxos) FreeMemory(seq int) {
	kv.px.Done(seq)
	pxCurrentMin := kv.px.Min()
	for kv.fp < pxCurrentMin {
		kv.serverLog[kv.fp] = *new(Op)
		kv.fp++
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.database = make(map[string]string)
	kv.serverLog = make([]Op, 5000, 10000)
	kv.cp = 0
	kv.ap = 0
	kv.fp = 0
	kv.filter = make(map[int64]int)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
