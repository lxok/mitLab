package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"net"
	"time"
)
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instance    []*Instance
	discardArgs map[int]int

	currentMin     int
	currentMax     int
	currentDoneArg int
}

//add
type Instance struct {
	seq      int
	status   Fate
	final    interface{}
	proposer Proposer
	acceptor Acceptor
}

type Proposer struct {
	phase int
	n     int
	value interface{}
}

type Acceptor struct {
	np int
	na int
	va interface{}
}

type ProposeArgs struct {
	InsNum  int
	Kinds   string
	N       int
	Value   interface{}
	DoneArg int
	Self    int
}

type ProposeReply struct {
	InsNum  int
	Status  Fate
	Result  string
	Np      int
	Na      int
	Va      interface{}
	Final   interface{}
	DoneArg int
}

const (
	Prepare = "Prepare"
	Accept  = "Accept"
	Decide  = "Decide"
)

const (
	OK     = "Ok"
	Reject = "Reject"
)

func (px *Paxos) Propose(seq int, v interface{}) {

	//px.mu.Lock()

	//fmt.Println("Propose value:", v)

	currentIns := px.instance[seq]

	//new instance
	if currentIns == nil {
		var ins Instance
		ins.seq = seq
		ins.status = Pending

		px.instance[seq] = &ins
		//px.instance[seq].acceptor.va = v
		px.instance[seq].proposer.value = v

		if px.currentMax < seq {
			px.currentMax = seq
		}
	}

	//px.mu.Unlock()

	if px.instance[seq].status != Pending {
		return
	}

	choosen := -1
	receivenp := -1
	var args ProposeArgs

	//start
	for px.instance[seq].status == Pending {

		if px.isdead() {
			break
		}

		//prepare

		prepareOK := 0

		if receivenp == -1 {
			px.instance[seq].proposer.n = choosen
			choosen++
		} else {
			receivenp++
			px.instance[seq].proposer.n = receivenp
		}

		args.InsNum = seq
		args.Kinds = Prepare
		args.N = px.instance[seq].proposer.n
		args.DoneArg = px.currentDoneArg
		args.Self = px.me

		//fmt.Println("Prepare args.Value:", v)

		for i := 0; i < len(px.peers); i++ {

			if px.instance[seq].status != Pending {
				args.Kinds = Decide
				args.InsNum = seq
				args.Value = px.instance[seq].final
			}

			var prepareReply ProposeReply

			if i != px.me {
				call(px.peers[i], "Paxos.AcceptHandler", &args, &prepareReply)
				//fmt.Println("Prepare seq:", seq, " from:", px.me, " value:", px.instance[seq].final, " to:", i, " reply.status:", prepareReply.Status)
			} else {
				px.AcceptHandler(&args, &prepareReply)
			}

			px.mu.Lock()

			if prepareReply.DoneArg != -1 {
				px.discardArgs[i] = prepareReply.DoneArg
			}

			px.mu.Unlock()

			if prepareReply.Status == Pending {

				if prepareReply.Result == OK {
					prepareOK++

					if prepareReply.Va != nil {
						px.instance[seq].proposer.value = prepareReply.Va
					}
				} else {

					if prepareReply.Np > receivenp {
						receivenp = prepareReply.Np
					}

				}

			} else {

				px.mu.Lock()

				if prepareReply.Status == Decided {
					px.instance[seq].seq = seq
					px.instance[seq].status = Decided
					px.instance[seq].final = prepareReply.Final

					px.instance[args.InsNum].acceptor.va = prepareReply.Final
					//return
				}

				px.mu.Unlock()

				if prepareReply.Status == Forgotten {
					px.instance[seq].seq = seq
					px.instance[seq].status = prepareReply.Status
					px.instance[seq].final = prepareReply.Final
					return
				}

			}

		}

		//accept

		//fmt.Println("prepareOK: ", prepareOK)

		if prepareOK > len(px.peers)/2 {

			acceptOK := 0
			args.InsNum = seq
			args.Kinds = Accept
			args.N = px.instance[seq].proposer.n
			args.Value = px.instance[seq].proposer.value
			args.DoneArg = px.currentDoneArg
			args.Self = px.me

			//fmt.Println("Accept args.Value:", px.instance[seq].proposer.value)

			for i := 0; i < len(px.peers); i++ {
				var acceptReply ProposeReply

				if i != px.me {
					call(px.peers[i], "Paxos.AcceptHandler", &args, &acceptReply)
					//fmt.Println("Accept seq:", seq, " from:", px.me, " to:", i, " reply.status:", acceptReply.Status)
				} else {
					px.AcceptHandler(&args, &acceptReply)
				}

				px.mu.Lock()

				if acceptReply.DoneArg != -1 {
					px.discardArgs[i] = acceptReply.DoneArg
				}

				px.mu.Unlock()

				if acceptReply.Status == Pending {

					if acceptReply.Result == OK {
						acceptOK++
					}

				} else {

					px.mu.Lock()
					if acceptReply.Status == Decided {
						px.instance[seq].seq = seq
						px.instance[seq].status = Decided
						px.instance[seq].final = acceptReply.Final

						px.instance[args.InsNum].acceptor.va = acceptReply.Final
						//return
					}
					px.mu.Unlock()

					if acceptReply.Status == Forgotten {
						px.instance[seq].seq = seq
						px.instance[seq].status = acceptReply.Status
						px.instance[seq].final = acceptReply.Final
						return
					}

				}

			}

			//decide
			if acceptOK > len(px.peers)/2 {

				//fmt.Println("decided")

				args.InsNum = seq
				args.Kinds = Decide
				args.N = px.instance[seq].proposer.n
				args.Value = px.instance[seq].proposer.value
				args.DoneArg = px.currentDoneArg
				args.Self = px.me

				//fmt.Println("Decided args.Value:", px.instance[seq].proposer.value)

				for i := 0; i < len(px.peers); i++ {
					var decidedReply ProposeReply
					if i != px.me {
						call(px.peers[i], "Paxos.AcceptHandler", &args, &decidedReply)
						//fmt.Println("Decide seq:", seq, " from:", px.me, " to:", i, " reply.status:", decidedReply.Status)
					} else {
						px.AcceptHandler(&args, &decidedReply)
					}
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (px *Paxos) AcceptHandler(args *ProposeArgs, reply *ProposeReply) error {

	//initia acceptor
	//new instance
	px.mu.Lock()

	if px.instance[args.InsNum] == nil {
		var ins Instance
		ins.seq = args.InsNum
		ins.status = Pending

		px.instance[args.InsNum] = &ins
		px.instance[args.InsNum].acceptor.np = -1
		px.instance[args.InsNum].acceptor.na = -1

		if px.currentMax < args.InsNum {
			px.currentMax = px.instance[args.InsNum].seq
		}
	}

	if args.DoneArg != -1 {
		px.discardArgs[args.Self] = args.DoneArg
	}

	px.mu.Unlock()

	if px.instance[args.InsNum].status != Pending {

		reply.InsNum = px.instance[args.InsNum].seq
		reply.Status = px.instance[args.InsNum].status
		reply.Final = px.instance[args.InsNum].final
		reply.DoneArg = px.currentDoneArg

	} else {

		if args.Kinds == Prepare {

			if args.N > px.instance[args.InsNum].acceptor.np {
				px.instance[args.InsNum].acceptor.np = args.N

				reply.InsNum = args.InsNum
				reply.Result = OK
				reply.Status = Pending
				reply.DoneArg = px.currentDoneArg

				reply.Na = px.instance[args.InsNum].acceptor.na
				reply.Va = px.instance[args.InsNum].acceptor.va

			} else {
				reply.Result = Reject
				reply.Np = px.instance[args.InsNum].acceptor.np
				reply.DoneArg = px.currentDoneArg
			}

		}

		if args.Kinds == Accept {

			if args.N >= px.instance[args.InsNum].acceptor.np {
				px.instance[args.InsNum].acceptor.np = args.N
				px.instance[args.InsNum].acceptor.na = args.N
				px.instance[args.InsNum].acceptor.va = args.Value

				reply.InsNum = args.InsNum
				reply.Result = OK
				reply.Status = Pending
				reply.DoneArg = px.currentDoneArg

				reply.Na = px.instance[args.InsNum].acceptor.na

			} else {
				reply.Result = Reject
			}

		}

		if args.Kinds == Decide {

			px.mu.Lock()

			px.instance[args.InsNum].acceptor.va = args.Value

			px.instance[args.InsNum].seq = args.InsNum
			px.instance[args.InsNum].final = args.Value
			px.instance[args.InsNum].status = Decided

			reply.Result = OK
			reply.Status = Decided
			reply.Final = px.instance[args.InsNum].final

			//fmt.Println("Acceptor decide seq:", args.InsNum, "to:", px.me, " value:", px.instance[args.InsNum].final)

			px.mu.Unlock()
		}

	}

	return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {

	// Your code here.
	go px.Propose(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.currentDoneArg = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.currentMax
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.

	px.mu.Lock()

	minimum := px.currentMin

	if len(px.discardArgs) == len(px.peers) {
		minimum = px.discardArgs[1]
		for _, v := range px.discardArgs {
			if v < minimum {
				minimum = v
			}
		}
	}

	px.currentMin = minimum

	//forget
	minCompDone := px.currentMin

	if px.currentDoneArg < px.currentMin {
		minCompDone = px.currentDoneArg
	}

	if px.currentMax < minCompDone {
		minCompDone = px.currentMax
	}

	for i := 0; i <= minCompDone; i++ {
		if px.instance[i] != nil && px.instance[i].status == Decided {
			px.instance[i].status = Forgotten
		}
	}

	px.mu.Unlock()

	return px.currentMin + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	px.mu.Lock()

	var state Fate
	var val interface{}

	if px.instance[seq] == nil {
		state = Pending
		val = nil
	} else {

		if px.instance[seq].status == Decided {
			state = Decided
			val = px.instance[seq].final

			//fmt.Println("seq:", seq, " state:", state, " val:", val, "px.instace[seq].final:", px.instance[seq].final)

		} else {
			state = px.instance[seq].status
			val = nil
		}
	}

	px.mu.Unlock()

	return state, val
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {

	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.mu.Lock()
	// Your initialization code here.
	px.instance = make([]*Instance, 500, 1000)
	px.currentMin = -1
	px.currentMax = -1
	px.currentDoneArg = -1
	px.discardArgs = make(map[int]int)

	px.mu.Unlock()

	//rpc register
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
