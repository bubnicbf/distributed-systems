package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// CUSTOM
	// 1. get next workerName from registerChan
	// 2. use custom_rpc.call() to send rpc to worker
	// call(workerAddress, Worker.DoTask, DoTaskArgs, nil)
	//

	var wg sync.WaitGroup
	wg.Add(ntasks)
	taskIdx := 0
	for taskIdx < ntasks {

		go func(idx int) {

			defer wg.Done()
			var doTaskArgs DoTaskArgs
			doTaskArgs.JobName = jobName
			if phase == mapPhase {
				doTaskArgs.File = mapFiles[idx]
			}
			doTaskArgs.Phase = phase
			doTaskArgs.TaskNumber = idx
			doTaskArgs.NumOtherPhase = n_other
			workerName := <-registerChan
			//doTaskCallRes := false
			doTaskCallRes := call(workerName, "Worker.DoTask", doTaskArgs, nil)
			go func() { registerChan <- workerName }()
			for doTaskCallRes == false {
				//fmt.Printf("Worker.DoTask %d Err: call failed\n", idx)
				time.Sleep(100 * time.Millisecond)
				workerName = <-registerChan
				doTaskCallRes = call(workerName, "Worker.DoTask", doTaskArgs, nil)
				go func() { registerChan <- workerName }()
			}
			//go func() { registerChan <- workerName }()
		}(taskIdx)
		taskIdx = taskIdx + 1
	}
	wg.Wait()
	fmt.Println("huhuhuhuhuhuuuh")
	fmt.Printf("Schedule: %v phase done\n", phase)
}
