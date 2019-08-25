package workerpool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSingleWorkerSingleTask(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	numberOfTasks := 1
	sut, ctorErr := New(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	runProcesses(sut, maxConcurrentWorkers, outChan, numberOfTasks)
	// for i := 0; i < numberOfTasks; i++ {
	// 	runProcess(sut, t, maxConcurrentWorkers, outChan)
	// }

	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkers)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestSingleWorker(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	numberOfTasks := 5
	sut, ctorErr := New(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < numberOfTasks; i++ {
		go runProcess(sut, t, maxConcurrentWorkers, outChan)
	}

	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkers)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestMultipleWorkers(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 3
	numberOfTasks := 10
	sut, ctorErr := New(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < numberOfTasks; i++ {
		runProcess(sut, t, maxConcurrentWorkers, outChan)
	}

	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkers)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestDecreaseWorkers(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkersInitial := 4
	maxConcurrentWorkersLater := 2
	numberOfTasks := 13
	sut, ctorErr := New(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < maxConcurrentWorkersInitial; i++ {
		go runProcess(sut, t, maxConcurrentWorkersInitial, outChan)
	}

	time.Sleep(time.Duration(100) * time.Millisecond)
	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkersInitial {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkersInitial)
	}

	for i := maxConcurrentWorkersInitial; i < numberOfTasks-5; i++ {
		go runProcess(sut, t, maxConcurrentWorkersLater, outChan)
	}

	time.Sleep(time.Duration(4000) * time.Millisecond)
	wokerCount = sut.WorkerCount()
	if wokerCount != maxConcurrentWorkersLater {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkersLater)
	}

	for i := numberOfTasks - 5; i < numberOfTasks; i++ {
		go runProcess(sut, t, 1, outChan)
	}

	wokerCount = sut.WorkerCount()
	if wokerCount != 1 {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, 1)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestIncreaseWorkers(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkersInitial := 1
	maxConcurrentWorkersLater := 5
	numberOfTasks := 16
	sut, ctorErr := New(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < 4; i++ {
		go runProcess(sut, t, maxConcurrentWorkersInitial, outChan)
	}
	time.Sleep(time.Duration(2000) * time.Millisecond)

	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkersInitial {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkersInitial)
	}

	for i := 4; i < numberOfTasks; i++ {
		go runProcess(sut, t, maxConcurrentWorkersLater, outChan)
	}

	wokerCount = sut.WorkerCount()
	if wokerCount != maxConcurrentWorkersLater {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkersLater)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestIncreaseToInfiniteWorkers(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkersInitial := 1
	maxConcurrentWorkersLater := 0
	numberOfTasks := 16
	sut, ctorErr := New(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < 4; i++ {
		go runProcess(sut, t, maxConcurrentWorkersInitial, outChan)
	}

	time.Sleep(time.Duration(2000) * time.Millisecond)
	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkersInitial {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkersInitial)
	}

	for i := 4; i < numberOfTasks; i++ {
		go runProcess(sut, t, maxConcurrentWorkersLater, outChan)
	}

	wokerCount = sut.WorkerCount()
	if wokerCount != 12 {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, 12)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestDecreaseFromInfiniteWorkers(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkersInitial := 0
	maxConcurrentWorkersLater := 1
	numberOfTasks := 10
	sut, ctorErr := New(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < 5; i++ {
		go runProcess(sut, t, maxConcurrentWorkersInitial, outChan)
	}

	time.Sleep(time.Duration(10) * time.Microsecond)
	wokerCount := sut.WorkerCount()
	if wokerCount != 5 {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, 5)
	}

	for i := 5; i < numberOfTasks; i++ {
		go runProcess(sut, t, maxConcurrentWorkersLater, outChan)
	}

	wokerCount = sut.WorkerCount()
	if wokerCount != maxConcurrentWorkersLater {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkersLater)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestManagerTimeOut(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	sut, ctorErr := New(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	outChan := make(chan TaskResult)
	task := taskFactory(8, "8s", outChan)
	sut.AssignTask(task, 1, time.Duration(30)*time.Second)

	wokerCount := sut.WorkerCount()
	if wokerCount != maxConcurrentWorkers {
		t.Fatalf("Worker count is %d, expected count is %d", wokerCount, maxConcurrentWorkers)
	}

	task2 := taskFactory(1, "1s", outChan)
	timeout, _ := sut.AssignTask(task2, 1, time.Duration(3)*time.Second)
	if !timeout {
		t.Errorf("Timeout should be reached")
	}
}

func TestWorkersReuse(t *testing.T) {
	t.Parallel()
	concurrentTasks := 10
	maxConcurrentWorkers := 0
	sut, ctorErr := New(maxConcurrentWorkers)
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
		if timeout, err := sut.AssignTask(task, 0, time.Duration(100*time.Second)); timeout || err != nil {
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
			if timeout, err := sut.AssignTask(task, 0, time.Duration(100*time.Second)); timeout || err != nil {
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

func runProcesses(p *Pool, maxConcurrentWorkers int, outChan chan TaskResult, tasksCount int) {
	task := taskFactory(1, "", outChan)
	var wg sync.WaitGroup
	wg.Add(tasksCount)
	for i := 0; i < tasksCount; i++ {
		go func() {
			p.AssignTask(task, maxConcurrentWorkers, time.Duration(30)*time.Second)
			wg.Done()
		}()
	}
	wg.Wait()
}

func runProcess(p *Pool, t *testing.T, maxConcurrentWorkers int, outChan chan TaskResult) {
	task := taskFactory(1, "", outChan)
	timeout, processErr := p.AssignTask(task, maxConcurrentWorkers, time.Duration(30)*time.Second)
	if processErr != nil {
		t.Fatalf("processErr: %s", processErr.Error())
	}
	if timeout {
		t.Fatalf("Timeout reached!")
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
