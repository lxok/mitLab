package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	//Your definitions here.
	serverLog   []Op
	cp          int //commit point
	ap          int //apply point
	fp          int //free memory point
	configPoint int //configs index point

	filter map[string]int
}

type Op struct {
	// Your data here.
	Seq     int
	Propser int

	Operation string
	Args      interface{}
	ArgOpId   string
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	//make a Op
	var ArgOp Op
	ArgOp.Propser = sm.me
	ArgOp.Operation = Join
	ArgOp.Args = *args
	ArgOp.ArgOpId = GenArgId(ArgOp.Operation, fmt.Sprintf("%d", ArgOp.Args.(JoinArgs).GID))

	//select a seq
	sm.GetFirstSeq()
	ArgOp.Seq = sm.cp

	//make a log, when change putOp' seq, must rewrite in log.
	sm.serverLog[sm.cp] = ArgOp
	sm.cp++

	//make a instance
	sm.px.Start(ArgOp.Seq, ArgOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val := sm.px.Status(ArgOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).ArgOpId == ArgOp.ArgOpId) {

		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = sm.px.Status(ArgOp.Seq)
			continue
		}

		// status == paxos.Decided
		sm.serverLog[ArgOp.Seq] = val.(Op)
		ArgOp.Seq = sm.cp
		sm.serverLog[sm.cp] = ArgOp
		sm.cp++

		sm.px.Start(ArgOp.Seq, ArgOp)
		status, val = sm.px.Status(ArgOp.Seq)
	}

	//execute actual operation
	sm.ExeConfigs(ArgOp.Seq)

	//free memory
	sm.FreeMemory(ArgOp.Seq)

	//return
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	//make a Op
	var ArgOp Op
	ArgOp.Propser = sm.me
	ArgOp.Operation = Leave
	ArgOp.Args = *args
	ArgOp.ArgOpId = GenArgId(ArgOp.Operation, fmt.Sprintf("%d", ArgOp.Args.(LeaveArgs).GID))

	//select a seq
	sm.GetFirstSeq()
	ArgOp.Seq = sm.cp

	//make a log, when change putOp' seq, must rewrite in log.
	sm.serverLog[sm.cp] = ArgOp
	sm.cp++

	//make a instance
	sm.px.Start(ArgOp.Seq, ArgOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val := sm.px.Status(ArgOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).ArgOpId == ArgOp.ArgOpId) {

		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = sm.px.Status(ArgOp.Seq)
			continue
		}

		// status == paxos.Decided
		sm.serverLog[ArgOp.Seq] = val.(Op)
		ArgOp.Seq = sm.cp
		sm.serverLog[sm.cp] = ArgOp
		sm.cp++

		sm.px.Start(ArgOp.Seq, ArgOp)
		status, val = sm.px.Status(ArgOp.Seq)
	}

	//execute actual operation
	sm.ExeConfigs(ArgOp.Seq)
	//	for k, v := range kv.database {
	//		fmt.Println("database:", kv.me, " key:", k, " value:", v)
	//	}

	//free memory
	sm.FreeMemory(ArgOp.Seq)

	//reply,return
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	//make a Op
	var ArgOp Op
	ArgOp.Propser = sm.me
	ArgOp.Operation = Move
	ArgOp.Args = *args
	ArgOp.ArgOpId = GenArgId(ArgOp.Operation, fmt.Sprintf("%d", ArgOp.Args.(MoveArgs).GID), fmt.Sprintf("%d", ArgOp.Args.(MoveArgs).Shard))

	//select a seq
	sm.GetFirstSeq()
	ArgOp.Seq = sm.cp

	//make a log, when change putOp' seq, must rewrite in log.
	sm.serverLog[sm.cp] = ArgOp
	sm.cp++

	//make a instance
	sm.px.Start(ArgOp.Seq, ArgOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val := sm.px.Status(ArgOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).ArgOpId == ArgOp.ArgOpId) {

		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = sm.px.Status(ArgOp.Seq)
			continue
		}

		// status == paxos.Decided
		sm.serverLog[ArgOp.Seq] = val.(Op)
		ArgOp.Seq = sm.cp
		sm.serverLog[sm.cp] = ArgOp
		sm.cp++

		sm.px.Start(ArgOp.Seq, ArgOp)
		status, val = sm.px.Status(ArgOp.Seq)
	}

	//execute actual operation
	sm.ExeConfigs(ArgOp.Seq)

	//free memory
	sm.FreeMemory(ArgOp.Seq)

	//reply,return
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	//make a Op
	var ArgOp Op
	ArgOp.Propser = sm.me
	ArgOp.Operation = Query
	ArgOp.Args = *args
	ArgOp.ArgOpId = GenArgId(ArgOp.Operation, fmt.Sprintf("%d", ArgOp.Args.(QueryArgs).Num))

	//select a seq
	sm.GetFirstSeq()
	ArgOp.Seq = sm.cp

	//make a log, when change putOp' seq, must rewrite in log.
	sm.serverLog[sm.cp] = ArgOp
	sm.cp++

	//make a instance
	sm.px.Start(ArgOp.Seq, ArgOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val := sm.px.Status(ArgOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).ArgOpId == ArgOp.ArgOpId) {
		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = sm.px.Status(ArgOp.Seq)
			continue
		}

		// status == paxos.Decided
		sm.serverLog[ArgOp.Seq] = val.(Op)
		ArgOp.Seq = sm.cp
		sm.serverLog[sm.cp] = ArgOp
		sm.cp++

		sm.px.Start(ArgOp.Seq, ArgOp)
		status, val = sm.px.Status(ArgOp.Seq)
	}

	//execute actual operation
	reply.Config = sm.ExeConfigs(ArgOp.Seq)

	//Query result
	//fmt.Println("Query result:","server:",sm.me,"seq:",ArgOp.Seq,"num:",ArgOp.Args.(QueryArgs).Num)

	//free memory
	sm.FreeMemory(ArgOp.Seq)

	//return
	return nil
}

func GenArgId(items ...string) string {
	result := ""
	for _, s := range items {
		result = result + s
	}
	return result
}

func (sm *ShardMaster) GetFirstSeq() {
	status, val := sm.px.Status(sm.cp)
	for status != paxos.Pending {
		if status == paxos.Decided {
			sm.serverLog[sm.cp] = val.(Op)
		}
		sm.cp++
		status, val = sm.px.Status(sm.cp)
	}
}

func (sm *ShardMaster) ExeConfigs(seq int) Config {
	//No operation is be filtered

	var result Config

	for sm.ap <= seq {

		//if _, ok := sm.filter[sm.serverLog[sm.ap].ArgOpId]; ok {
		//	fmt.Println("filter:", " server:", sm.me, " seq:", sm.ap)
		//	sm.ap++
		//	continue
		//}

		switch sm.serverLog[sm.ap].Operation {

		case Join:
			var config Config

			//groups
			config.Groups = make(map[int64][]string)
			tmpCounter := make(map[int64]int)
			MaxMinCounter := make(map[int]int)
			for k, v := range sm.configs[sm.configPoint-1].Groups {
				config.Groups[k] = v
				tmpCounter[k] = 0
			}
			config.Groups[sm.serverLog[sm.ap].Args.(JoinArgs).GID] = sm.serverLog[sm.ap].Args.(JoinArgs).Servers

			//shards
			average := NShards / len(config.Groups)
			if NShards%len(config.Groups) != 0 {
				average++
			}

			max := average
			min := average - 1
			MaxMinCounter[min] = max*len(config.Groups) - NShards
			MaxMinCounter[max] = len(config.Groups) - MaxMinCounter[min]

			for _, gid := range sm.configs[sm.configPoint-1].Shards {
				tmpCounter[gid]++
			}

			//compute how many shards should be movited of a group.
			for gid, _ := range tmpCounter {
				if tmpCounter[gid] >= max {
					if MaxMinCounter[max] > 0 {
						tmpCounter[gid] = tmpCounter[gid] - max
						MaxMinCounter[max]--
					} else {
						tmpCounter[gid] = tmpCounter[gid] - min
					}
				} else {
					tmpCounter[gid] = tmpCounter[gid] - min
				}
			}

			for k, gid := range sm.configs[sm.configPoint-1].Shards {
				if tmpCounter[gid] > 0 || gid == 0 {
					config.Shards[k] = sm.serverLog[sm.ap].Args.(JoinArgs).GID
					tmpCounter[gid]--
				} else {
					config.Shards[k] = gid
				}
			}

			//fmt.Println("Join finish: server:", sm.me, "seq:", sm.ap, " gid:", sm.serverLog[sm.ap].Args.(JoinArgs).GID, " average:", average)
			//for k, gid := range config.Shards {
			//	fmt.Println("Shards:", " k:", k, " gid:", gid)
			//}
			//fmt.Print("groups:")
			//for gid, _ := range config.Groups {
			//	fmt.Print(gid, " ")
			//}
			//fmt.Println("")

			//log config in sm.configs
			config.Num = sm.configPoint
			sm.configs = append(sm.configs, config)
			sm.configPoint++

			//log Op in filter
			sm.filter[sm.serverLog[sm.ap].ArgOpId] = 1

		case Leave:
			var config Config

			//groups
			config.Groups = make(map[int64][]string)
			tmpCounter := make(map[int64]int)
			MaxMinCounter := make(map[int]int)
			leaveSet := make(map[int]int)
			for k, v := range sm.configs[sm.configPoint-1].Groups {
				config.Groups[k] = v
				tmpCounter[k] = 0
			}
			delete(config.Groups, sm.serverLog[sm.ap].Args.(LeaveArgs).GID)

			//shards
			average := NShards / len(config.Groups)
			if NShards%len(config.Groups) != 0 {
				average++
			}
			max := average
			min := average - 1
			MaxMinCounter[min] = max*len(config.Groups) - NShards
			MaxMinCounter[max] = len(config.Groups) - MaxMinCounter[min]

			for shardIndex, gid := range sm.configs[sm.configPoint-1].Shards {
				if gid == sm.serverLog[sm.ap].Args.(LeaveArgs).GID {
					leaveSet[shardIndex] = 1
				}
				config.Shards[shardIndex] = gid
				tmpCounter[gid]++
			}
			delete(tmpCounter, sm.serverLog[sm.ap].Args.(LeaveArgs).GID)

			for gid, _ := range tmpCounter {
				if MaxMinCounter[max] > 0 {
					tmpCounter[gid] = max - tmpCounter[gid]
					MaxMinCounter[max]--
					for i := 0; i < tmpCounter[gid]; i++ {
						for shardIndex, _ := range leaveSet {
							config.Shards[shardIndex] = gid
							delete(leaveSet, shardIndex)
							break
						}
					}
				}
			}

			//fmt.Println("Leav finish: server:", sm.me, "seq:", sm.ap, " gid:", sm.serverLog[sm.ap].Args.(LeaveArgs).GID, " average:", average)
			//for k, gid := range config.Shards {
			//	fmt.Println("k:", k, " gid:", gid)
			//}

			//log config in sm.configs
			config.Num = sm.configPoint
			sm.configs = append(sm.configs, config)
			sm.configPoint++

			//log Op in filter
			sm.filter[sm.serverLog[sm.ap].ArgOpId] = 1

		case Move:
			var config Config

			//fmt.Println("Move :", " server:", sm.me, " seq:", sm.ap, " shard:", sm.serverLog[sm.ap].Args.(MoveArgs).Shard, " gid:", sm.serverLog[sm.ap].Args.(MoveArgs).GID)

			//groups
			config.Groups = make(map[int64][]string)
			for k, v := range sm.configs[sm.configPoint-1].Groups {
				config.Groups[k] = v
			}

			//shards
			for k, gid := range sm.configs[sm.configPoint-1].Shards {
				config.Shards[k] = gid
			}
			config.Shards[sm.serverLog[sm.ap].Args.(MoveArgs).Shard] = sm.serverLog[sm.ap].Args.(MoveArgs).GID

			//log config in sm.configs
			config.Num = sm.configPoint
			sm.configs = append(sm.configs, config)
			sm.configPoint++

		default:
			//Operation == Query
		}

		if sm.ap == seq && sm.serverLog[sm.ap].Operation == Query {
			if sm.serverLog[sm.ap].Args.(QueryArgs).Num == -1 || sm.serverLog[sm.ap].Args.(QueryArgs).Num > len(sm.configs)-1 {
				result = sm.configs[len(sm.configs)-1]
			} else {
				result = sm.configs[sm.serverLog[sm.ap].Args.(QueryArgs).Num]
			}

			//fmt.Println("Query:", " server:", sm.me, " seq:", sm.ap, " num:", sm.serverLog[sm.ap].Args.(QueryArgs).Num)
			//fmt.Print("groups:")
			//for gid, _ := range sm.configs[sm.configPoint-1].Groups {
			//	fmt.Print(gid, " ")
			//}
			//fmt.Println("")
		}

		sm.ap++
	}

	return result
}

func (sm *ShardMaster) FreeMemory(seq int) {
	sm.px.Done(seq)
	pxCurrentMin := sm.px.Min()
	for sm.fp < pxCurrentMin {
		sm.serverLog[sm.fp] = *new(Op)
		sm.fp++
	}
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	// Your initialization code here.
	sm.serverLog = make([]Op, 5000*100, 10000*100)
	sm.cp = 0
	sm.ap = 0
	sm.fp = 0
	sm.configPoint = 1
	sm.filter = make(map[string]int)
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
