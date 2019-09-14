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
	initComplete   bool
}

//New returns pool instance
func New(options ...func(*Pool) error) (*Pool, error) {
	pool := &Pool{
		taskQueue:      make(chan *Task),
		vacancyChanged: make(chan void),
		workers:        make(map[*Worker]void),
		observers:      list.New(),
	}
	for _, opt := range options {
		if err := opt(pool); err != nil {
			return nil, err
		}
	}
	pool.initComplete = true
	return pool, nil
}

//AssignTask assigns new task to pool. It blocks execution until
//there is a free worker to execute task or timeout is reached
func (p *Pool) AssignTask(request *Task, timeout time.Duration) (bool, error) {
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

//SetConcurrencyLimit setups pool workers capacity thus
//bounding concurrency rate. Zero have special meaning of
//having no upper bound. Default is zero. This functuion
//can be called at any time. Setting this value below the
//current actual amount of workers cause exceeding workers
//will be released after completion of their tasks.
func (p *Pool) SetConcurrencyLimit(lim int) error {
	if lim < 0 {
		return fmt.Errorf("Negative value for concurrency limit is not allowed. Was %d", lim)
	}
	p.vacanciesLock.Lock()
	defer p.vacanciesLock.Unlock()
	if p.initComplete && p.maxWorkerCount == lim {
		return nil
	}
	p.maxWorkerCount = lim

	if !p.isInfiniteWorkersAllowed() {
		currentWorkersCount := len(p.workers)
		p.vacancies = make(chan void, lim)
		//Create amount of vacancies equal to maxWorkerCount - currentWorkersCount
		for i := currentWorkersCount; i < lim; i++ {
			p.vacancies <- signal
		}
		i := 0
		//Stop exceeded workres
		for w := range p.workers {
			if i >= currentWorkersCount-lim {
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
	return nil
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
