package mapreduce

import (
	"fmt"
	"reflect"
	"sync"
)

type workerInfo = struct {
	success bool
	worker string
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	// Make 1 channel for communication with each Map task process
	channels := make([](chan workerInfo), ntasks, ntasks)
	for i := 0; i < ntasks; i++ {
		channels[i] = make(chan workerInfo)
	}

	var wg sync.WaitGroup
	
	// Define doTask for the corresponding phase
	var doTask func(int)
	switch phase {
	case mapPhase:
		doTask = func(i int) {
			mapTask(jobName, i, mapFiles[i], nReduce, channels[i], &wg)
		}
	case reducePhase:
		doTask = func(i int) {
			reduceTask(jobName, i, nMap, channels[i], &wg)
		}
	}

	// Fork off a process for each Map task that needs to be completed
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go doTask(i)
	}

	// Prepare selectCases for non-blocking read from each channel
	selectCases := make([]reflect.SelectCase, ntasks+1, ntasks+1)
	for i := 0; i < ntasks; i++ {
		selectCases[i].Dir = reflect.SelectRecv
		selectCases[i].Chan = reflect.ValueOf(channels[i])
	}
	selectCases[ntasks].Dir = reflect.SelectDefault

	// Load available workers from registerChan, and make slice of
	// task numbers that are waiting to be scheduled:
	workers := getWorkerAddresses(registerChan)
	toSchedule := make([]int, ntasks, ntasks)
	for i := 0; i < ntasks; i++ {
		toSchedule[i] = i
	}
	// While there are still unfinished (ongoing & unscheduled) tasks, do:
	for unfinishedTasks := ntasks; unfinishedTasks > 0; {
		// Read from a task's channel, if applicable
		chosen, recv, recvOk := reflect.Select(selectCases)
		if recvOk {
			// If something was read, add returned worker to available slice
			recvInfo := recv.Interface().(workerInfo)
			workers = append(workers, recvInfo.worker)
			// If task succeeded, decrement unfinished counter
			// Else, append task to toSchedule slice
			if recvInfo.success {
				unfinishedTasks--
			} else {
				toSchedule = append(toSchedule, chosen)
			}
		}
		// Check if more workers are available since last read of registerChan
		workers = append(workers, getWorkerAddresses(registerChan)...)
		// Assign tasks to available workers
		for len(workers) > 0 && len(toSchedule) > 0 {
			w := workers[0]
			workers = workers[1:]
			task := toSchedule[0]
			toSchedule = toSchedule[1:]
			channels[task] <- workerInfo{false, w}
		}
	}

	wg.Wait()
	
	fmt.Printf("Schedule: %v done\n", phase)
}

// Reads all strings waiting on channel, and returns a slice containing them
func getWorkerAddresses(channel chan string) []string {
	var workers []string
	for {
		select {
		case worker := <- channel:
			workers = append(workers, worker)
		default:
			return workers
		}
	}
}

func mapTask(jobName string, mapTask int, mapFile string, nReduce int, channel chan workerInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	taskArgs := DoTaskArgs{jobName, mapFile, mapPhase, mapTask, nReduce}
	doTask(taskArgs, channel)
}

func reduceTask(jobName string, reduceTask int, nMap int, channel chan workerInfo, wg *sync.WaitGroup) {
	defer wg.Done()
	taskArgs := DoTaskArgs{jobName, "", reducePhase, reduceTask, nMap}
	doTask(taskArgs, channel)
}

func doTask(taskArgs DoTaskArgs, channel chan workerInfo) {
	// Repeat until RPC succeeds:
	for success := false; !success; {
		// Wait to receive worker address from scheduler
		wi := <- channel
		// Invoke RPC to worker
		success = call(wi.worker, "Worker.DoTask", taskArgs, nil)
		// Return worker address & RPC success status to scheduler
		channel <- workerInfo{success, wi.worker}
	}
}
