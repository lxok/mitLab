package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	currentView    viewservice.View
	isPrimary      bool
	isBackup       bool
	hasInitial     bool
	shouldpushdata bool

	data        map[string]string
	getFinished map[int64]int
	putFinished map[int64]int
	//backupPutFinished map[int64]int
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//fmt.Println("get")
	//fmt.Println(pb.data)

	//fmt.Println("lock")
	pb.mu.Lock()

	//id := args.Id
	key := args.Key

	//_, finish := pb.getFinished[id]

	if pb.isPrimary /*&& !finish*/ {
		if v, ok := pb.data[key]; ok {
			reply.Value = v
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
		//pb.getFinished[id] = 1
	}

	if !pb.isPrimary {
		reply.Err = ErrWrongServer
	}
	//fmt.Println("reply: ", reply.Value)
	//fmt.Println("unlock")
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.

	//fmt.Println("lock")
	//fmt.Println("here putappend")
	//fmt.Println("value", args.Value)
	//fmt.Println("pb.me:", pb.me, "pb.isBackup:", pb.isBackup, " pb.hasInitial:", pb.hasInitial, " pb.isPrimary:", pb.isPrimary)
	//fmt.Println("finish:", finish)
	pb.mu.Lock()
	//fmt.Println("put:", args.Key, " ", args.Value, " ", args.Op, " ", pb.me, " ", pb.currentView.Backup)
	if pb.isPrimary {
		key := args.Key
		value := args.Value
		op := args.Op
		id := args.Id

		if _, finish := pb.putFinished[id]; !finish {
			if pb.currentView.Backup == "" {
				if op == Put {
					pb.data[key] = value
				}
				if op == Append {
					if v, ok := pb.data[key]; ok {
						pb.data[key] = v + value
					} else {
						pb.data[key] = value
					}
				}
				pb.putFinished[id] = 1
				//fmt.Println("put success--no backup ", "key:", key, " value:", pb.data[key])
				reply.Err = OK
			}
			//fmt.Println("Backup:", pb.currentView.Backup)
			if pb.currentView.Backup != "" {
				//forward
				var forwardReply PutAppendReply
				var forwardAgrs PutAppendArgs
				forwardAgrs.Id = id
				forwardAgrs.Key = key
				forwardAgrs.Value = value
				forwardAgrs.Op = op

				call(pb.currentView.Backup, "PBServer.PutForward", &forwardAgrs, &forwardReply)
				if forwardReply.Err == OK {
					if op == Put {
						pb.data[key] = value
					}
					if op == Append {
						if v, ok := pb.data[key]; ok {
							pb.data[key] = v + value
						} else {
							pb.data[key] = value
						}
					}
					reply.Err = OK
					pb.putFinished[id] = 1
					//fmt.Println("put success--with backup ", "key:", key, " value:", pb.data[key])
				} else {
					reply.Err = forwardReply.Err
					//fmt.Println("put fail:forward fail")
				}
			}
		} else {
			reply.Err = OK
			//fmt.Println("put success--has finish ", "key:", args.Key, " value:", pb.data[key])
		}
	} else {
		reply.Err = ErrWrongServer
		//fmt.Println("put fail: not primary")
	}

	//fmt.Println("put end")
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) PutForward(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	if pb.isBackup && pb.hasInitial {

		key := args.Key
		value := args.Value
		op := args.Op
		id := args.Id

		if _, finish := pb.putFinished[id]; !finish && op == Append {

			if v, ok := pb.data[key]; ok {
				pb.data[key] = v + value
			} else {
				pb.data[key] = value
			}
			//fmt.Println("put forward success--", "key:", args.Key, " value:", pb.data[args.Key])
			//check
			pb.putFinished[id] = 1
			reply.Err = OK

		} else if op == Put {

			pb.data[key] = value

			//fmt.Println("put forward success--", "key:", args.Key, " value:", pb.data[args.Key])
			//check
			if pb.data[key] == args.Value {
				pb.putFinished[id] = 1
				reply.Err = OK
			} else {
				reply.Err = BackupWriteWrong
			}

		} else {
			reply.Err = OK
			//fmt.Println("put forword success--has finish", "key:", args.Key, " value:", pb.data[args.Key])
		}

	} else {
		if !pb.isBackup {
			reply.Err = ErrWrongServer
		} else {
			reply.Err = ErrNoKey
		}
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) GetComplete(args *GetCompleteArgs, reply *GetCompleteReply) error {
	pb.mu.Lock()
	if pb.currentView.Backup == pb.me {
		pb.data = args.Database
		pb.putFinished = args.Filter
		reply.Err = OK
		pb.hasInitial = true
	} else {
		reply.Err = NotBecomeBackup
	}

	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	var receiveView viewservice.View
	receiveView, _ = pb.vs.Ping(pb.currentView.Viewnum)

	if receiveView.Primary != pb.me {
		pb.isPrimary = false
		pb.shouldpushdata = false
	}
	if receiveView.Backup != pb.me {
		pb.isBackup = false
		pb.hasInitial = false
	}
	if receiveView.Primary == pb.me {
		pb.isPrimary = true
		pb.hasInitial = false
	}
	if receiveView.Backup == pb.me {
		pb.isBackup = true
		pb.shouldpushdata = false
	}
	if receiveView.Backup != pb.me && receiveView.Primary != pb.me {
		pb.hasInitial = false
		pb.shouldpushdata = false
	}

	if pb.isPrimary && receiveView.Backup != pb.currentView.Backup {
		pb.shouldpushdata = true
	}

	//database primary->backup
	if receiveView.Backup != "" && pb.shouldpushdata && pb.isPrimary {
		var args GetCompleteArgs
		var reply GetCompleteReply
		database := make(map[string]string)
		filter := make(map[int64]int)
		for k, v := range pb.data {
			database[k] = v
		}
		for k, v := range pb.putFinished {
			filter[k] = v
		}
		args.Database = database
		args.Filter = filter
		call(receiveView.Backup, "PBServer.GetComplete", &args, &reply)
		if reply.Err == OK {
			//fmt.Println("has push data")
			pb.shouldpushdata = false
		}
	}

	pb.currentView = receiveView

	//fmt.Println("server pb.isPrimary ", pb.isPrimary)
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	// Your pb.* initializations here.

	pb.isPrimary = false
	pb.isBackup = false
	pb.data = make(map[string]string)
	pb.putFinished = make(map[int64]int)
	pb.getFinished = make(map[int64]int)
	pb.hasInitial = false
	pb.shouldpushdata = false

	pb.currentView.Viewnum = 0
	pb.currentView.Backup = ""
	pb.currentView.Primary = ""

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
