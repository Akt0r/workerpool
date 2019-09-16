package workerpool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

func TestConstantWorkers(t *testing.T) {
	t.Parallel()
	workersTasks := []struct {
		workers, tasks int
	}{
		{1, 1},
		{1, 5},
		{3, 10},
	}
	outChan := make(chan TaskResult)
	for _, wt := range workersTasks {
		sut, ctorErr := New(WithConcurrencyLimitOf(wt.workers))
		if ctorErr != nil {
			t.Fatalf("ctorErr: %s", ctorErr.Error())
		}
		runProcesses(sut, outChan, wt.tasks, wt.workers)
		wokerCount := sut.WorkerCount()
		if wokerCount != wt.workers {
			t.Fatalf("Worker count is %d, expected count is %d", wokerCount, wt.workers)
		}

		for i := 0; i < wt.tasks; i++ {
			t.Log(<-outChan)
		}
	}
}

func TestDecreaseWorkers(t *testing.T) {
	t.Parallel()
	workersTasks := []struct {
		workers, tasks int
	}{
		{4, 4},
		{2, 4},
		{1, 4},
	}

	wdisp := make(chan void, workersTasks[0].workers)
	obs := &ObserverStub{
		WorkerDisposedCallback: func(p *Pool) {
			wdisp <- signal
		},
		AllWorkersDisposedCallback: func(p *Pool) {},
		WorkerCreatedCallback:      func(p *Pool) {},
	}
	sut, ctorErr := New(WithConcurrencyLimitOf(workersTasks[0].workers))
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	sut.RegisterObserver(obs)

	outChan := make(chan TaskResult)

	runProcesses(sut, outChan, workersTasks[0].tasks, workersTasks[0].workers)
	wokerCount := sut.WorkerCount()
	if wokerCount != workersTasks[0].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[0].workers)
	}
	for i := 0; i < workersTasks[0].workers; i++ {
		t.Log(<-outChan)
	}

	sut.SetConcurrencyLimit(workersTasks[1].workers)
	runProcesses(sut, outChan, workersTasks[1].tasks, workersTasks[1].workers)
	for i := 0; i < workersTasks[0].workers-workersTasks[1].workers; i++ {
		<-wdisp
	}
	wokerCount = sut.WorkerCount()
	if wokerCount != workersTasks[1].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[1].workers)
	}

	sut.SetConcurrencyLimit(workersTasks[2].workers)
	runProcesses(sut, outChan, workersTasks[2].tasks, 0)

	for i := 0; i < workersTasks[1].tasks+workersTasks[2].tasks; i++ {
		t.Log(<-outChan)
	}
	for i := 0; i < workersTasks[1].workers-workersTasks[2].workers; i++ {
		<-wdisp
	}
	wokerCount = sut.WorkerCount()
	if wokerCount != workersTasks[2].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[2].workers)
	}
}

func TestIncreaseWorkers(t *testing.T) {
	t.Parallel()
	workersTasks := []struct {
		workers, tasks int
	}{
		{1, 4},
		{5, 11},
	}
	sut, ctorErr := New(WithConcurrencyLimitOf(workersTasks[0].workers))
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	outChan := make(chan TaskResult)

	runProcesses(sut, outChan, workersTasks[0].tasks, workersTasks[0].workers)
	wokerCount := sut.WorkerCount()
	if wokerCount != workersTasks[0].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[0].workers)
	}

	sut.SetConcurrencyLimit(workersTasks[1].workers)
	runProcesses(sut, outChan, workersTasks[1].tasks, workersTasks[1].workers-workersTasks[0].workers)
	wokerCount = sut.WorkerCount()
	if wokerCount != workersTasks[1].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[1].workers)
	}

	for i := 0; i < workersTasks[0].tasks+workersTasks[1].tasks; i++ {
		t.Log(<-outChan)
	}
}

func TestIncreaseToInfiniteWorkers(t *testing.T) {
	t.Parallel()
	workersTasks := []struct {
		workers, tasks int
	}{
		{1, 4},
		{0, 16},
	}
	sut, ctorErr := New(WithConcurrencyLimitOf(workersTasks[0].workers))
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	outChan := make(chan TaskResult)

	runProcesses(sut, outChan, workersTasks[0].tasks, workersTasks[0].workers)
	wokerCount := sut.WorkerCount()
	if wokerCount != workersTasks[0].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[0].workers)
	}

	totalWorkers := workersTasks[0].tasks + workersTasks[1].tasks
	sut.SetConcurrencyLimit(workersTasks[1].workers)
	runProcesses(sut, outChan, workersTasks[1].tasks, workersTasks[1].tasks)
	wokerCount = sut.WorkerCount()
	if wokerCount != totalWorkers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, totalWorkers)
	}

	for i := 0; i < workersTasks[0].tasks+workersTasks[1].tasks; i++ {
		t.Log(<-outChan)
	}
}

func TestDecreaseFromInfiniteWorkers(t *testing.T) {
	t.Parallel()
	workersTasks := []struct {
		workers, tasks int
	}{
		{0, 5},
		{1, 5},
	}
	sut, ctorErr := New()
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	wdisp := make(chan void, workersTasks[0].workers)
	obs := &ObserverStub{
		WorkerDisposedCallback: func(p *Pool) {
			wdisp <- signal
		},
		AllWorkersDisposedCallback: func(p *Pool) {},
		WorkerCreatedCallback:      func(p *Pool) {},
	}
	sut.RegisterObserver(obs)
	outChan := make(chan TaskResult)
	runProcesses(sut, outChan, workersTasks[0].tasks, workersTasks[0].tasks)

	wokerCount := sut.WorkerCount()
	if wokerCount != workersTasks[0].tasks {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[0].tasks)
	}

	sut.SetConcurrencyLimit(workersTasks[1].workers)
	for i := 0; i < workersTasks[0].tasks; i++ {
		t.Log(<-outChan)
	}
	for i := 0; i < workersTasks[0].tasks-workersTasks[1].workers; i++ {
		<-wdisp
	}
	wokerCount = sut.WorkerCount()
	if wokerCount != workersTasks[1].workers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, workersTasks[1].workers)
	}

	runProcesses(sut, outChan, workersTasks[1].tasks, workersTasks[1].workers)

	for i := 0; i < workersTasks[1].tasks; i++ {
		t.Log(<-outChan)
	}
}

func TestManagerTimeOut(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	sut, ctorErr := New(WithConcurrencyLimitOf(maxConcurrentWorkers))
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	outChan := make(chan TaskResult)
	task := taskFactory(8, "8s", outChan)
	sut.AssignTask(task, time.Duration(30)*time.Second)

	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkers)
	}

	task2 := taskFactory(1, "1s", outChan)
	timeout, _ := sut.AssignTask(task2, time.Duration(3)*time.Second)
	if !timeout {
		t.Errorf("Timeout should be reached")
	}
}

func TestWorkersReuse(t *testing.T) {
	t.Parallel()
	concurrentTasks := 10
	sut, ctorErr := New()
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	outChan := make(chan TaskResult)
	task := &Task{
		Task: func() TaskResult {
			time.Sleep(time.Duration(100) * time.Millisecond)
			return "OK"
		},
		Result: outChan,
	}
	//lets create concurrentTasks amount of workers
	for i := 0; i < concurrentTasks; i++ {
		if timeout, err := sut.AssignTask(task, time.Duration(100*time.Second)); timeout || err != nil {
			t.Fatalf("Task assigning failed or timeout has been reached")
		}
	}

	wokerCount := sut.WorkerCount()
	if wokerCount != concurrentTasks {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, concurrentTasks)
	}

	//now wait for workers complete their tasks
	for i := 0; i < concurrentTasks; i++ {
		<-outChan
	}
	//so we have concurrentTasks amount of idle workers
	//from now on no new workers should be created
	//for incoming tasks
	t.Log(sut.String())
	restrictiveObs := &ObserverStub{}
	restrictiveObs.WorkerCreatedCallback = func(p *Pool) {
		t.Log(p.String())
		t.Fatalf("Oh no. Worker has been created.")
	}
	restrictiveObs.WorkerDisposedCallback = func(p *Pool) {}
	restrictiveObs.AllWorkersDisposedCallback = func(p *Pool) {}

	sut.RegisterObserver(restrictiveObs)
	for i := 0; i < 10; i++ {
		//lets create concurrentTasks amount of workers
		for i := 0; i < concurrentTasks; i++ {
			if timeout, err := sut.AssignTask(task, time.Duration(100*time.Second)); timeout || err != nil {
				t.Fatalf("Task assigning failed or timeout has been reached")
			}
		}
		//now wait for workers complete their tasks
		for i := 0; i < concurrentTasks; i++ {
			<-outChan
		}
	}
}

type ObserverStub struct {
	WorkerCreatedCallback      func(*Pool)
	WorkerDisposedCallback     func(*Pool)
	AllWorkersDisposedCallback func(*Pool)
}

func (o *ObserverStub) WorkerCreated(p *Pool) {
	o.WorkerCreatedCallback(p)
}

func (o *ObserverStub) WorkerDisposed(p *Pool) {
	o.WorkerDisposedCallback(p)
}

func (o *ObserverStub) AllWorkersDisposed(p *Pool) {
	o.AllWorkersDisposedCallback(p)
}

func runProcesses(p *Pool, outChan chan TaskResult, tasksCount int, waitForCount int) {
	task := taskFactory(1, "", outChan)
	ctx := context.TODO()
	sem := semaphore.NewWeighted(int64(tasksCount))
	if err := sem.Acquire(ctx, int64(tasksCount)); err != nil {
		panic(fmt.Sprintf("Failed to acquire semaphore: %v", err))
	}
	for i := 0; i < tasksCount; i++ {
		go func() {
			p.AssignTask(task, time.Duration(30)*time.Second)
			sem.Release(1)
		}()
	}
	if err := sem.Acquire(ctx, int64(waitForCount)); err != nil {
		panic(fmt.Sprintf("Failed to acquire semaphore: %v", err))
	}
}

func taskFactory(delay int64, id string, outChan chan TaskResult) *Task {
	taskF := func() TaskResult {
		start := time.Now()
		time.Sleep(time.Duration(delay) * time.Second)
		return fmt.Sprintf("Task %s started at %s", id, start.Format("15:04:05"))
	}
	return &Task{
		Task:   taskF,
		Result: outChan,
	}
}
