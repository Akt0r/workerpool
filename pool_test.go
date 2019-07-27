package workerpool

import (
	"fmt"
	"testing"
	"time"
)

func TestSingleWorkerSingleTask(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	numberOfTasks := 1
	m, ctorErr := NewPool(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < numberOfTasks; i++ {
		go runProcess(m, t, maxConcurrentWorkers, outChan)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestSingleWorker(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	numberOfTasks := 5
	m, ctorErr := NewPool(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < numberOfTasks; i++ {
		go runProcess(m, t, maxConcurrentWorkers, outChan)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestMultipleWorkers(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 3
	numberOfTasks := 10
	m, ctorErr := NewPool(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < numberOfTasks; i++ {
		go runProcess(m, t, maxConcurrentWorkers, outChan)
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
	m, ctorErr := NewPool(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < maxConcurrentWorkersInitial; i++ {
		go runProcess(m, t, maxConcurrentWorkersInitial, outChan)
	}
	time.Sleep(time.Duration(100) * time.Millisecond)
	for i := maxConcurrentWorkersInitial; i < numberOfTasks-5; i++ {
		go runProcess(m, t, maxConcurrentWorkersLater, outChan)
	}
	time.Sleep(time.Duration(4000) * time.Millisecond)
	for i := numberOfTasks - 5; i < numberOfTasks; i++ {
		go runProcess(m, t, 1, outChan)
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
	m, ctorErr := NewPool(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < 4; i++ {
		go runProcess(m, t, maxConcurrentWorkersInitial, outChan)
	}
	time.Sleep(time.Duration(2000) * time.Millisecond)
	for i := 4; i < numberOfTasks; i++ {
		go runProcess(m, t, maxConcurrentWorkersLater, outChan)
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
	m, ctorErr := NewPool(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < 4; i++ {
		go runProcess(m, t, maxConcurrentWorkersInitial, outChan)
	}
	time.Sleep(time.Duration(2000) * time.Millisecond)
	for i := 4; i < numberOfTasks; i++ {
		go runProcess(m, t, maxConcurrentWorkersLater, outChan)
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
	m, ctorErr := NewPool(maxConcurrentWorkersInitial)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}

	outChan := make(chan TaskResult)
	for i := 0; i < 5; i++ {
		go runProcess(m, t, maxConcurrentWorkersInitial, outChan)
	}
	time.Sleep(time.Duration(10) * time.Microsecond)
	for i := 5; i < numberOfTasks; i++ {
		go runProcess(m, t, maxConcurrentWorkersLater, outChan)
	}

	for i := 0; i < numberOfTasks; i++ {
		t.Log(<-outChan)
	}
}

func TestManagerTimeOut(t *testing.T) {
	t.Parallel()
	maxConcurrentWorkers := 1
	m, ctorErr := NewPool(maxConcurrentWorkers)
	if ctorErr != nil {
		t.Fatalf("ctorErr: %s", ctorErr.Error())
	}
	outChan := make(chan TaskResult)
	task := taskFactory(8, "8s", outChan)
	m.AssignTask(task, 1, time.Duration(30)*time.Second)
	task2 := taskFactory(1, "1s", outChan)
	timeout, _ := m.AssignTask(task2, 1, time.Duration(3)*time.Second)
	if !timeout {
		t.Errorf("Timeout should be reached")
	}
}

func TestWorkersReuse(t *testing.T) {
	t.Parallel()
	concurrentTasks := 10
	maxConcurrentWorkers := 0
	sut, ctorErr := NewPool(maxConcurrentWorkers)
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

func runProcess(m *Pool, t *testing.T, maxConcurrentWorkers int, outChan chan TaskResult) {
	task := taskFactory(1, "", outChan)
	timeout, processErr := m.AssignTask(task, maxConcurrentWorkers, time.Duration(30)*time.Second)
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
