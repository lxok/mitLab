package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Seq       int
	Propser   int
	Id        int64
	ConfigNum int
	ShardNum  int
	SendState []bool

	Operation string
	Key       string
	Value     string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	currentConfig shardmaster.Config
	responsible   map[int]bool

	database  map[string]string
	serverLog []Op
	cp        int //commit point
	ap        int //apply point
	fp        int //free memory point
	filter    map[int64]int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Get()", "Config:", kv.currentConfig.Num)

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

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Get()-Exe", "Config:", kv.currentConfig.Num)

	//execute actual operation, reply the value
	result := kv.ExeDatabase(getOp.Seq)

	//verify if is responsible for the request
	v, ok := kv.responsible[key2shard(args.Key)]
	if !ok {
		reply.Err = ErrWrongGroup
	} else if ok && !v {
		reply.Err = ErrNotInitial
	} else /* if ok && v*/ {
		//continue exe
		if result == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = result
		}
	}

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Get()-Result:", reply.Err, "Value:", reply.Value, "Config:", kv.currentConfig.Num)

	//free memory
	kv.FreeMemory(getOp.Seq)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "PutAppend()", "Config:", kv.currentConfig.Num)
	//fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "PutAppend()1", "Config:", kv.currentConfig.Num)

	//make a Op
	var putOp Op
	putOp.Id = args.Id
	putOp.Propser = kv.me
	putOp.Operation = args.Op
	putOp.Key = args.Key
	putOp.Value = args.Value

	//fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "PutAppend()-SelectSeq", "Config:", kv.currentConfig.Num)

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

	//fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "PutAppend()-StartInstance", "Config:", kv.currentConfig.Num)

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

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "PutAppend()-Exe", "Config:", kv.currentConfig.Num)

	//execute actual operation
	kv.ExeDatabase(putOp.Seq)
	//	for k, v := range kv.database {
	//		fmt.Println("database:", kv.me, " key:", k, " value:", v)
	//	}

	//free memory
	kv.FreeMemory(putOp.Seq)

	//reply
	v, ok := kv.responsible[key2shard(args.Key)]
	if !ok {
		reply.Err = ErrWrongGroup
	} else if ok && !v {
		reply.Err = ErrNotInitial
	} else /* if ok && v*/ {
		reply.Err = OK
	}

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "PutAppend()-Result:", reply.Err, "Config:", kv.currentConfig.Num)

	return nil
}

func (kv *ShardKV) ConfigChange(receiveConfig *shardmaster.Config) {

	// 1. paxos agreement 2. send shard

	if kv.currentConfig.Num == receiveConfig.Num {
		return
	}

	kv.mu.Lock()
	//defer kv.mu.Unlock()

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "ConfigChange()", "Config:", kv.currentConfig.Num, "ChangeConfig:", receiveConfig.Num)

	//make a Op
	var configOp Op
	configOp.Propser = kv.me
	configOp.Operation = ConfigChange
	configOp.ConfigNum = receiveConfig.Num
	configOp.Id = nrand()

	//select a seq
	status, val := kv.px.Status(kv.cp)
	for status != paxos.Pending {
		if status == paxos.Decided {
			kv.serverLog[kv.cp] = val.(Op)
		}
		kv.cp++
		status, val = kv.px.Status(kv.cp)
	}

	configOp.Seq = kv.cp

	//make a log, when change configOp' seq, must rewrite in log.
	kv.serverLog[kv.cp] = configOp
	kv.cp++

	//make a instance
	kv.px.Start(configOp.Seq, configOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val = kv.px.Status(configOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).Id == configOp.Id) {

		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = kv.px.Status(configOp.Seq)
			continue
		}
		if val.(Op).Id != configOp.Id {
			kv.serverLog[configOp.Seq] = val.(Op)
			configOp.Seq = kv.cp
			kv.serverLog[kv.cp] = configOp
			kv.cp++

			kv.px.Start(configOp.Seq, configOp)
			status, val = kv.px.Status(configOp.Seq)
		}
	}

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "ConfigChangeStartExe()", "Config:", kv.currentConfig.Num, "ChangeConfig:", receiveConfig.Num, "Seq:", configOp.Seq)

	//execute actual operation
	kv.ExeDatabase(configOp.Seq)

	//create a old kv.currentConfig
	var shadowCurrentConfig shardmaster.Config
	shadowResponsible := make(map[int]bool)

	if kv.currentConfig.Num < configOp.ConfigNum {

		shadowCurrentConfig.Num = kv.currentConfig.Num
		for key, value := range kv.currentConfig.Shards {
			shadowCurrentConfig.Shards[key] = value
		}
		shadowCurrentConfig.Groups = make(map[int64][]string)
		for key, value := range kv.currentConfig.Groups {
			shadowCurrentConfig.Groups[key] = value
		}

		for key, value := range kv.responsible {
			shadowResponsible[key] = value
		}

		//update kv.currentConfig
		kv.AcceptorConfigUpdate(receiveConfig)

	}

	kv.mu.Unlock()

	//send shard, if need
	if shadowCurrentConfig.Num < configOp.ConfigNum {
		kv.ProposerStartSendShard(&shadowCurrentConfig, shadowResponsible, receiveConfig)
	}

	//free memory
	kv.FreeMemory(configOp.Seq)
}

func (kv *ShardKV) ShardInitial(arg *ShardInitalArgs, reply *ShardInitialReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	//make a Op
	var InitialOp Op
	InitialOp.Propser = kv.me
	InitialOp.Operation = ShardIntial
	InitialOp.ConfigNum = arg.ConfigNum
	InitialOp.ShardNum = arg.ShardNum
	InitialOp.SendState = arg.SendState
	InitialOp.Id = nrand()

	//select a seq
	status, val := kv.px.Status(kv.cp)
	for status != paxos.Pending {
		if status == paxos.Decided {
			kv.serverLog[kv.cp] = val.(Op)
		}
		kv.cp++
		status, val = kv.px.Status(kv.cp)
	}

	InitialOp.Seq = kv.cp

	//make a log, when change InitialOp' seq, must rewrite in log.
	kv.serverLog[kv.cp] = InitialOp
	kv.cp++

	//make a instance
	kv.px.Start(InitialOp.Seq, InitialOp)

	//get result, if be seized, start a instance again, until be decided
	to := 10 * time.Millisecond
	status, val = kv.px.Status(InitialOp.Seq)
	for !(status == paxos.Decided && val != nil && val.(Op).Id == InitialOp.Id) {

		if status != paxos.Decided {
			if to < 100*time.Millisecond {
				to *= 2
			}
			time.Sleep(to)
			status, val = kv.px.Status(InitialOp.Seq)
			continue
		}
		if val.(Op).Id != InitialOp.Id {
			kv.serverLog[InitialOp.Seq] = val.(Op)
			InitialOp.Seq = kv.cp
			kv.serverLog[kv.cp] = InitialOp
			kv.cp++

			kv.px.Start(InitialOp.Seq, InitialOp)
			status, val = kv.px.Status(InitialOp.Seq)
		}
	}

	//execute actual operation
	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "ConfigChangeStartExe()", "Config:", kv.currentConfig.Num, "ChangeConfig:", InitialOp.ConfigNum, "ShardNum:", InitialOp.ShardNum, "Seq:", InitialOp.Seq)

	kv.ExeDatabase(InitialOp.Seq)

	//free memory
	kv.FreeMemory(InitialOp.Seq)

	//reply and return
	reply.Err = OK

	return nil
}

func (kv *ShardKV) ExeDatabase(seq int) string {

	result := ""

	for kv.ap <= seq {

		//fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Exe()", "Ap:", kv.ap, "Cp:", kv.cp, "Operation:", kv.serverLog[kv.ap].Operation, "Config:", kv.currentConfig.Num)

		if _, ok := kv.filter[kv.serverLog[kv.ap].Id]; ok {

			fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Exe()-Filtered", "Ap:", kv.ap, "Seq:", seq, "Cp:", kv.cp, "Operation:", kv.serverLog[kv.ap].Operation, "Key:", kv.serverLog[kv.ap].Key, "Shard:", key2shard(kv.serverLog[kv.ap].Key), "Config:", kv.currentConfig.Num)

			kv.ap++
			continue
		}

		if kv.serverLog[kv.ap].Operation == Get || kv.serverLog[kv.ap].Operation == Put || kv.serverLog[kv.ap].Operation == Append {
			if v, ok := kv.responsible[key2shard(kv.serverLog[kv.ap].Key)]; ok && v {
				//execute
			} else {
				fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Exe()-NotInitial", "Ap:", kv.ap, "Seq:", seq, "Cp:", kv.cp, "Operation:", kv.serverLog[kv.ap].Operation, "Key:", kv.serverLog[kv.ap].Key, "Shard:", key2shard(kv.serverLog[kv.ap].Key), "Config:", kv.currentConfig.Num)

				kv.ap++
				continue
			}
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

			fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Exe()", "Ap:", kv.ap, "Seq:", seq, "Cp:", kv.cp, "Operation:", "ExeInDatabase", "Key:", kv.serverLog[kv.ap].Key, "Value:", kv.serverLog[kv.ap].Value, "Shard:", key2shard(kv.serverLog[kv.ap].Key), "Config:", kv.currentConfig.Num)

		} else if kv.serverLog[kv.ap].Operation == Get {
			if kv.ap == seq {
				if v, ok := kv.database[kv.serverLog[kv.ap].Key]; ok {
					result = v
				}
			}
		} else if kv.serverLog[kv.ap].Operation == ConfigChange {
			if kv.ap != seq && kv.serverLog[kv.ap].ConfigNum > kv.currentConfig.Num /*filter*/ {
				config := kv.sm.Query(kv.serverLog[kv.ap].ConfigNum)
				kv.AcceptorConfigUpdate(&config)
			}
		} else {
			//kv.serverLog[kv.ap].Operation == ShardInitial
			if kv.serverLog[kv.ap].ConfigNum <= kv.currentConfig.Num && kv.serverLog[kv.ap].SendState[kv.me] {
				if value, ok := kv.responsible[kv.serverLog[kv.ap].ShardNum]; !value && ok {
					kv.responsible[kv.serverLog[kv.ap].ShardNum] = true
				}
			}
		}

		fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "Exe()", "Ap:", kv.ap, "Seq:", seq, "Cp:", kv.cp, "Operation:", kv.serverLog[kv.ap].Operation, "Key:", kv.serverLog[kv.ap].Key, "Shard:", key2shard(kv.serverLog[kv.ap].Key), "ShardNum:", kv.serverLog[kv.ap].ShardNum, "Config:", kv.currentConfig.Num)

		kv.ap++
	}

	return result
}

//update acceptor's kv.currentConfig and kv.responsible
func (kv *ShardKV) AcceptorConfigUpdate(receiveConfig *shardmaster.Config) {

	//filter the dulplicate config argeement
	if kv.currentConfig.Num >= receiveConfig.Num {
		return
	}

	tmpResponsible := make(map[int]bool)
	for shard, gid := range receiveConfig.Shards {
		if gid == kv.gid {
			tmpResponsible[shard] = false
		}
	}

	shadowResponsible := make(map[int]bool)
	for k, v := range kv.responsible {
		shadowResponsible[k] = v
	}

	//additional shards
	for shard, _ := range tmpResponsible {
		if _, ok := shadowResponsible[shard]; !ok {
			if receiveConfig.Num == 1 {
				shadowResponsible[shard] = true
			} else {
				shadowResponsible[shard] = false
			}
		}
	}

	//reduced shards
	for shard, _ := range shadowResponsible {
		if _, ok := tmpResponsible[shard]; !ok {
			delete(shadowResponsible, shard)
		}
	}

	//update kv.currentConfig and kv.responsible
	kv.currentConfig = *receiveConfig
	kv.responsible = shadowResponsible
}

func (kv *ShardKV) FreeMemory(seq int) {
	kv.px.Done(seq)
	pxCurrentMin := kv.px.Min()
	for kv.fp < pxCurrentMin {
		kv.serverLog[kv.fp] = *new(Op)
		kv.fp++
	}
}

//update proposer's kv.currentConfig and kv.responsible
func (kv *ShardKV) ProposerStartSendShard(oldCurrentConfig *shardmaster.Config, oldResponsible map[int]bool, receiveConfig *shardmaster.Config) {

	//filter the dulplicate config argeement
	if oldCurrentConfig.Num >= receiveConfig.Num {
		return
	}

	tmpResponsible := make(map[int]bool)
	for shard, gid := range receiveConfig.Shards {
		if gid == kv.gid {
			tmpResponsible[shard] = false
		}
	}

	shadowResponsible := make(map[int]bool)
	for k, v := range oldResponsible {
		shadowResponsible[k] = v
	}

	//additional shards
	for shard, _ := range tmpResponsible {
		if _, ok := shadowResponsible[shard]; !ok {
			if receiveConfig.Num == 1 {
				shadowResponsible[shard] = true
			} else {
				shadowResponsible[shard] = false
			}
		}
	}

	//reduced shards
	for shard, _ := range shadowResponsible {
		if _, ok := tmpResponsible[shard]; !ok {
			//send shard
			kv.SendShard(oldCurrentConfig, oldResponsible, shard, receiveConfig) //only proposer can invoke this
		}
	}
}

//get a shard from other group' server
func (kv *ShardKV) GetShard(args *SendShardArgs, reply *SendShardReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "GetShard()", "Config:", kv.currentConfig.Num, "GetCofig:", args.ConfigNum, "ShardNum:", args.ShardNum)

	if args.ConfigNum <= kv.currentConfig.Num {
		if value, ok := kv.responsible[args.ShardNum]; !value && ok {
			for key, value := range args.Data {
				kv.database[key] = value
			}
			for key, value := range args.Filter {
				kv.filter[key] = value
			}
			//kv.responsible[args.ShardNum] = true
		}
		reply.Err = OK
	}

	return nil
}

//send a shard from other group' server
func (kv *ShardKV) SendShard(oldCurrentConfig *shardmaster.Config, oldResponsible map[int]bool, shardNum int, receiveConfig *shardmaster.Config) {

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "SendShard()", "Config:", oldCurrentConfig.Num, "ShardNum:", shardNum)

	//if not be initial, call other server which is in the same group send the shard
	if value, ok := oldResponsible[shardNum]; !value && ok {
		sisterServers := oldCurrentConfig.Groups[kv.gid]

		var args AcceptorSendShardArgs
		args.ShardNum = shardNum
		args.ConfigNum = receiveConfig.Num

		hasSend := false
		for !hasSend {
			for index, server := range sisterServers {
				if index != kv.me {
					var reply AcceptorSendShardReply
					success := call(server, "ShardKV.AcceptorSendShard", &args, &reply)
					if success && reply.Err == OK {
						hasSend = true
						break
					}
				}
			}
			if !hasSend {
				time.Sleep(300 * time.Millisecond)
			}
		}
	} else {

		//shard has be initialed
		gid := receiveConfig.Shards[shardNum]
		servers, ok := receiveConfig.Groups[gid]

		kv.mu.Lock()

		sendData := make(map[string]string)

		for key, value := range kv.database {
			if key2shard(key) == shardNum {
				sendData[key] = value
			}
		}

		sendFilter := make(map[int64]int)

		for key, value := range kv.filter {
			sendFilter[key] = value
		}

		kv.mu.Unlock()

		var args SendShardArgs
		args.ConfigNum = receiveConfig.Num
		args.ShardNum = shardNum
		args.Data = sendData
		args.Filter = sendFilter

		success := 0
		waitSendServers := make([]bool, len(servers))
		for index, _ := range waitSendServers {
			waitSendServers[index] = false
		}

		fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "SendShardsTo:")
		for _, server := range servers {
			fmt.Println("SendShardsTo:", server)
		}

		/*
			fmt.Print("GID:", kv.gid, " Server:", kv.me, "", " SendShard()-Invoke", " :[")
			for index, value := range waitSendServers {
				fmt.Print(" ", index, ":", value)
			}
			fmt.Println("]")
		*/

		if ok {
			for success < len(servers)/2+1 {
				for index, value := range waitSendServers {
					if !value {
						var reply SendShardReply
						fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "SendShard()-Invoke", "Config:", oldCurrentConfig.Num, "ShardNum:", shardNum, "RecSrv:", servers[index])
						answer := call(servers[index], "ShardKV.GetShard", &args, &reply)
						if answer && reply.Err == OK {
							fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "SendShard()-OK", "Config:", oldCurrentConfig.Num, "ShardNum:", shardNum, "RecSrv:", servers[index])
							success++
							waitSendServers[index] = true
						}
					}
				}

				fmt.Print("GID:", kv.gid, " Server:", kv.me, "", " SendShard()-Invoke", " :[")
				for index, value := range waitSendServers {
					fmt.Print(" ", index, ":", value)
				}
				fmt.Println("]")

				time.Sleep(100 * time.Millisecond)
			}
		}

		//send shard finish, call the target server to intial
		var shardInitialArgs ShardInitalArgs
		shardInitialArgs.ConfigNum = receiveConfig.Num
		shardInitialArgs.ShardNum = shardNum
		shardInitialArgs.SendState = waitSendServers

		sendShardCount := 0

		for sendShardCount == 0 {
			for _, targetServer := range servers {
				var shardInitialReply ShardInitialReply
				call(targetServer, "ShardKV.ShardInitial", &shardInitialArgs, &shardInitialReply)
				if shardInitialReply.Err == OK {
					sendShardCount++
				}
			}

			if sendShardCount == 0 {
				time.Sleep(150 * time.Millisecond)
			}
		}
	}
}

func (kv *ShardKV) AcceptorSendShard(acceptorArgs *AcceptorSendShardArgs, acceptorReply *AcceptorSendShardReply) error {

	if value, ok := kv.responsible[acceptorArgs.ShardNum]; !value && ok {
		return nil
	}

	var receiveConfig shardmaster.Config
	shardNum := acceptorArgs.ShardNum

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "AcceptorSendShard()", "Config:", kv.currentConfig.Num, "ShardNum:", shardNum)

	//if kv.currentConfig has changed, if changed, invoke shardMaster' Query()
	if kv.currentConfig.Num == acceptorArgs.ConfigNum {
		receiveConfig = kv.currentConfig
	} else {
		receiveConfig = kv.sm.Query(acceptorArgs.ConfigNum)
	}
	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "AcceptorSendShard()-GetConfig", "Config:", kv.currentConfig.Num, "ShardNum:", shardNum)

	//send Shard
	gid := receiveConfig.Shards[shardNum]
	servers, ok := receiveConfig.Groups[gid]

	kv.mu.Lock()

	sendData := make(map[string]string)
	for key, value := range kv.database {
		if key2shard(key) == shardNum {
			sendData[key] = value
		}
	}

	sendFilter := make(map[int64]int)
	for key, value := range kv.filter {
		sendFilter[key] = value
	}

	kv.mu.Unlock()

	var args SendShardArgs
	args.ConfigNum = receiveConfig.Num
	args.ShardNum = shardNum
	args.Data = sendData
	args.Filter = sendFilter

	success := 0
	waitSendServers := make([]bool, len(servers))
	for index, _ := range waitSendServers {
		waitSendServers[index] = false
	}

	if ok {

		fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "AcceptorSendShard()-InsertOk", "Config:", kv.currentConfig.Num, "ShardNum:", shardNum)
		for success < len(servers)/2+1 {
			for index, value := range waitSendServers {
				if !value {
					var reply SendShardReply
					fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "AcceptorSendShard()-Invoke", "Config:", kv.currentConfig.Num, "ShardNum:", shardNum, "RecSrv:", servers[index])
					answer := call(servers[index], "ShardKV.GetShard", &args, &reply)
					if answer && reply.Err == OK {
						fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "AcceptorSendShard()-OK", "Config:", kv.currentConfig.Num, "ShardNum:", shardNum, "RecSrv:", servers[index])
						success++
						waitSendServers[index] = true
					}
				}
			}

			fmt.Print("GID:", kv.gid, " Server:", kv.me, "", " AcceptorSendShard()-Invoke", " :[")
			for index, value := range waitSendServers {
				fmt.Print(" ", index, ":", value)
			}
			fmt.Println("]")

			time.Sleep(100 * time.Millisecond)
		}
	}

	//send shard finish, call the target server to  intial
	var shardInitialArgs ShardInitalArgs
	shardInitialArgs.ConfigNum = receiveConfig.Num
	shardInitialArgs.ShardNum = shardNum
	shardInitialArgs.SendState = waitSendServers

	sendShardCount := 0

	for sendShardCount == 0 {
		for _, targetServer := range servers {
			var shardInitialReply ShardInitialReply
			call(targetServer, "ShardKV.ShardInitial", &shardInitialArgs, &shardInitialReply)
			if shardInitialReply.Err == OK {
				sendShardCount++
			}
		}

		if sendShardCount == 0 {
			time.Sleep(150 * time.Millisecond)
		}
	}

	acceptorReply.Err = OK

	fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "AcceptorSendShard()-Reslut:", acceptorReply.Err, "Config:", kv.currentConfig.Num, "ShardNum:", shardNum)
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	//fmt.Println("GID:", kv.gid, "Server:", kv.me, "", "tick()")

	receiveConfig := kv.sm.Query(-1)

	//fmt.Println("receiveConfig.Num:", receiveConfig.Num)
	//fmt.Println("kv.currentConfig.Num:", kv.currentConfig.Num)

	if receiveConfig.Num != kv.currentConfig.Num {
		//start a paxos agreement
		kv.ConfigChange(&receiveConfig)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.responsible = make(map[int]bool)

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
