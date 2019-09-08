package workerpool

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type void struct{}

var signal void

//Pool provides infrastructure for asynchronous task execution
//with support of bounded concurrency
type Pool struct {
	taskQueue      chan *Task
	vacancyChanged chan void
	vacancies      chan void
	maxWorkerCount int
	workers        map[*Worker]void
	vacanciesLock  sync.RWMutex
	observers      *list.List
	obsLock        sync.RWMutex
}

//New returns pool instance
func New(maxWorkerCount int) (*Pool, error) {
	if maxWorkerCount < 0 {
		return nil, fmt.Errorf("Negative value %v for maxWorkerCount is not allowed", maxWorkerCount)
	}
	pool := &Pool{
		taskQueue:      make(chan *Task),
		vacancyChanged: make(chan void),
		workers:        make(map[*Worker]void),
		observers:      list.New(),
	}
	pool.setupVacancies(maxWorkerCount, true)
	return pool, nil
}

//AssignTask assigns new task to pool. It blocks execution until
//there is a free worker to execute task or timeout is reached
func (p *Pool) AssignTask(request *Task, maxWorkerCount int, timeout time.Duration) (bool, error) {
	if maxWorkerCount < 0 {
		return false, fmt.Errorf("Negative value %v for maxWorkerCount is not allowed", maxWorkerCount)
	}
	p.setupVacancies(maxWorkerCount, false)

	vacancies := p.vacancies
	for {
		//We prioritize worker reuse over creating
		//new workers
		select {
		case p.taskQueue <- request:
			{
				//Task successfully assigned
				return false, nil
			}
		default:
		}

		vacancies = p.vacancies
		//No free workers at the moment. So let's
		//wait for someone
		select {
		case p.taskQueue <- request:
			{
				//Task successfully assigned
				return false, nil
			}
		case <-p.vacancyChanged:
			{
				//Refresh vacancies channel
				vacancies = p.vacancies
			}
		case <-vacancies:
			{
				w := p.createWorker()
				go func() {
					if !w.ProcessTask(request) {
						w.Listen(p.taskQueue)
					}
				}()
				//Task successfully assigned
				return false, nil
			}
		case <-time.After(timeout):
			{
				//No free worker found
				return true, nil
			}
		}
	}
}

//RegisterObserver registers observer that will get notifications
//about pool state changes
func (p *Pool) RegisterObserver(o Observer) error {
	if o == nil {
		return fmt.Errorf("Observer can't be nil")
	}
	p.obsLock.Lock()
	p.observers.PushBack(o)
	p.obsLock.Unlock()
	return nil
}

//WorkerCount returns total worker count,
//including both idle and running ones
func (p *Pool) WorkerCount() int {
	p.vacanciesLock.RLock()
	defer p.vacanciesLock.RUnlock()
	return len(p.workers)
}

//String provides pool state for debug
func (p *Pool) String() string {
	return fmt.Sprintf("Worker count: %d", p.WorkerCount())
}

//createWorker creates a new worker and add it
//in pools workers collection
func (p *Pool) createWorker() *Worker {
	workerStopped := func(w *Worker) {
		p.vacanciesLock.Lock()
		delete(p.workers, w)
		if p.maxWorkerCount > len(p.workers) {
			p.vacancies <- signal
		}
		p.notifyObservers(func(o Observer) {
			o.WorkerDisposed(p)
		})
		if len(p.workers) == 0 {
			p.notifyObservers(func(o Observer) {
				o.AllWorkersDisposed(p)
			})
		}
		p.vacanciesLock.Unlock()
	}
	p.vacanciesLock.Lock()
	defer p.vacanciesLock.Unlock()
	worker := &Worker{
		StopSignal:   make(chan void, 1),
		StopCallback: workerStopped,
	}
	p.workers[worker] = signal
	if p.isInfiniteWorkersAllowed() {
		p.vacancies <- signal
	}
	p.notifyObservers(func(o Observer) {
		o.WorkerCreated(p)
	})
	return worker
}

//setupVacancies changes maximum amount of tasks that pool
//can process simultaneously
func (p *Pool) setupVacancies(maxWorkerCount int, isInit bool) {
	p.vacanciesLock.Lock()
	defer p.vacanciesLock.Unlock()
	if !isInit && p.maxWorkerCount == maxWorkerCount {
		return
	}
	p.maxWorkerCount = maxWorkerCount

	if !p.isInfiniteWorkersAllowed() {
		currentWorkersCount := len(p.workers)
		p.vacancies = make(chan void, maxWorkerCount)
		//Create amount of vacancies equal to maxWorkerCount - currentWorkersCount
		for i := currentWorkersCount; i < maxWorkerCount; i++ {
			p.vacancies <- signal
		}
		i := 0
		//Stop exceeded workres
		for w := range p.workers {
			if i >= currentWorkersCount-maxWorkerCount {
				break
			}
			select {
			case w.StopSignal <- signal:
			default:
				//Continue if stop signal to that worker has been sent already
			}
			i++
		}
	} else {
		p.vacancies = make(chan void, 1)
		p.vacancies <- signal
	}
	p.notifyForVacancies()
}

//notifyForVacancies send signal about new vacancies to all goroutines
//that wait for free worker
func (p *Pool) notifyForVacancies() {
	for {
		select {
		case p.vacancyChanged <- signal:
			break
		default:
			return
		}
	}
}

//isInfiniteWorkersAllowed checks for unbounded concurrency pool mode
func (p *Pool) isInfiniteWorkersAllowed() bool {
	return p.maxWorkerCount == 0
}

//notifyObservers notifies all observers about some event
func (p *Pool) notifyObservers(callback func(o Observer)) {
	p.obsLock.RLock()
	defer p.obsLock.RUnlock()
	for elem := p.observers.Front(); elem != nil; elem = elem.Next() {
		obs := elem.Value.(Observer)
		callback(obs)
	}
}
