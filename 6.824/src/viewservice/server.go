package viewservice

import (
	//	"container/list"
	"net"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	recentTime     map[string]time.Time
	needChange     bool
	readyChange    bool
	canChange      bool
	currentViewNum uint
	currentView    View
	nextView       View
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()

	theViewName := args.Me
	theViewNum := args.Viewnum

	vs.recentTime[theViewName] = time.Now()

	if (theViewName == vs.currentView.Primary && theViewNum == vs.currentView.Viewnum) || vs.currentViewNum == 0 {
		vs.canChange = true
	}

	//the restart ping -- Ping(0)
	if theViewNum == 0 && vs.currentViewNum != 0 && theViewName == vs.currentView.Primary {
		vs.needChange = true
	}

	if vs.needChange {
		if vs.currentView.Primary == "" && len(vs.recentTime) > 0 {
			var newNextView View
			for k, _ := range vs.recentTime {
				newNextView.Primary = k
				newNextView.Backup = ""
				newNextView.Viewnum = vs.currentView.Viewnum + 1
				break
			}
			vs.nextView = newNextView
			vs.readyChange = true
			vs.needChange = false
		}

		if vs.currentView.Backup == "" && len(vs.recentTime) > 1 {
			var newNextView View
			for k, _ := range vs.recentTime {
				if k != vs.currentView.Primary {
					newNextView.Primary = vs.currentView.Primary
					newNextView.Backup = k
					newNextView.Viewnum = vs.currentView.Viewnum + 1
					break
				}
			}
			vs.nextView = newNextView
			vs.readyChange = true
			vs.needChange = false
		}

		if vs.currentView.Primary != "" && vs.currentView.Backup != "" {
			if _, ok := vs.recentTime[vs.currentView.Primary]; !ok {
				var newNextView View
				newNextView.Primary = vs.currentView.Backup
				if len(vs.recentTime) > 1 {
					for k, _ := range vs.recentTime {
						if k != vs.currentView.Backup {
							newNextView.Backup = k
							break
						}
					}
				} else {
					newNextView.Backup = ""
				}
				newNextView.Viewnum = vs.currentView.Viewnum + 1
				vs.nextView = newNextView
				vs.readyChange = true
				vs.needChange = false
			}

			if _, ok := vs.recentTime[vs.currentView.Backup]; !ok {
				var newNextView View
				newNextView.Primary = vs.currentView.Primary
				if len(vs.recentTime) > 1 {
					for k, _ := range vs.recentTime {
						if k != vs.currentView.Primary {
							newNextView.Backup = k
							break
						}
					}
				} else {
					newNextView.Backup = ""
				}
				newNextView.Viewnum = vs.currentView.Viewnum + 1
				vs.nextView = newNextView
				vs.readyChange = true
				vs.needChange = false
			}
		}

		if theViewNum == 0 && vs.currentViewNum != 0 && (theViewName == vs.currentView.Primary || theViewName == vs.currentView.Backup) {
			if theViewName == vs.currentView.Primary {
				var newNextView View
				newNextView.Primary = vs.currentView.Backup
				if len(vs.recentTime) > 1 {
					for k, _ := range vs.recentTime {
						if k != vs.currentView.Backup {
							newNextView.Backup = k
							break
						}
					}
				} else {
					newNextView.Backup = ""
				}
				newNextView.Viewnum = vs.currentView.Viewnum + 1
				vs.nextView = newNextView
				vs.readyChange = true
				vs.needChange = false
			} else {
				var newNextView View
				newNextView.Primary = vs.currentView.Primary
				if len(vs.recentTime) > 1 {
					for k, _ := range vs.recentTime {
						if k != vs.currentView.Primary {
							newNextView.Backup = k
							break
						}
					}
				} else {
					newNextView.Backup = ""
				}
				newNextView.Viewnum = vs.currentView.Viewnum + 1
				vs.nextView = newNextView
				vs.readyChange = true
				vs.needChange = false
			}
		}
	}

	//change current View
	if vs.canChange && (vs.readyChange || vs.currentViewNum == 0) {
		vs.currentView.Primary = vs.nextView.Primary
		vs.currentView.Backup = vs.nextView.Backup
		vs.currentView.Viewnum = vs.nextView.Viewnum
		vs.readyChange = false
		vs.needChange = false
		vs.canChange = false
		vs.currentViewNum++
	}

	reply.View.Viewnum = vs.currentView.Viewnum
	reply.View.Primary = vs.currentView.Primary
	reply.View.Backup = vs.currentView.Backup

	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()

	reply.View.Primary = vs.currentView.Primary
	reply.View.Backup = vs.currentView.Backup
	reply.View.Viewnum = vs.currentView.Viewnum

	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()

	var available bool = false
	var haveUsedServer = 0

	if vs.currentView.Primary != "" {
		haveUsedServer++
	}
	if vs.currentView.Backup != "" {
		haveUsedServer++
	}
	if len(vs.recentTime)-haveUsedServer > 0 {
		available = true
	}

	if vs.currentView.Backup == "" && available {
		vs.needChange = true
	}

	if time.Now().Sub(vs.recentTime[vs.currentView.Primary]) > DeadPings*PingInterval {
		vs.needChange = true
		delete(vs.recentTime, vs.currentView.Primary)
	}

	if time.Now().Sub(vs.recentTime[vs.currentView.Backup]) > DeadPings*PingInterval {
		vs.needChange = true
		delete(vs.recentTime, vs.currentView.Backup)
	}

	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me

	// Your vs.* initializations here.
	vs.recentTime = make(map[string]time.Time)
	vs.needChange = true
	vs.readyChange = false
	vs.canChange = false
	vs.currentViewNum = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
