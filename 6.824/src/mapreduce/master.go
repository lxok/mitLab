package mapreduce

import (
	"container/list"
)
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	mapChan := make(chan int)
	reduceChan := make(chan int)

	//map job allocate
	for i := 0; i < mr.nMap; i++ {
		go func(jobNumber int) {

			var registerWorker string
			var args DoJobArgs
			var reply DoJobReply

			args.File = mr.file
			args.JobNumber = jobNumber
			args.NumOtherPhase = mr.nReduce
			args.Operation = Map

			var ok bool = false
			for !ok {
				registerWorker = <-mr.registerChannel
				ok = call(registerWorker, "Worker.DoJob", &args, &reply)
			}
			mapChan <- 1
			mr.registerChannel <- registerWorker
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapChan
	}

	//reduce job allocate
	for i := 0; i < mr.nReduce; i++ {
		go func(jobNumber int) {

			var registerWorker string
			var args DoJobArgs
			var reply DoJobReply

			args.File = mr.file
			args.JobNumber = jobNumber
			args.NumOtherPhase = mr.nMap
			args.Operation = Reduce
			var ok bool = false
			for !ok {
				registerWorker = <-mr.registerChannel
				ok = call(registerWorker, "Worker.DoJob", &args, &reply)
			}
			reduceChan <- 1
			mr.registerChannel <- registerWorker
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceChan
	}

	return mr.KillWorkers()
}
