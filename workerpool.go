package workerpool

import (
	"fmt"
	"sync"
	"time"
)

type void struct{}

var signal void

// Pool позволяет распределять работу на несколько
// исполнителей
type Pool struct {
	taskQueue      chan *Task
	vacancyChanged chan void
	vacancies      chan void
	maxWorkerCount int
	workers        map[*Worker]void
	vacanciesLock  sync.RWMutex
}

//NewPool возвращает новый инициализированный объект Pool
func NewPool(maxWorkerCount int) (*Pool, error) {
	if maxWorkerCount < 0 {
		return nil, fmt.Errorf("Negative value %v for maxWorkerCount is not allowed", maxWorkerCount)
	}
	pool := &Pool{
		taskQueue:      make(chan *Task),
		vacancyChanged: make(chan void),
		workers:        make(map[*Worker]void),
	}
	pool.setupVacancies(maxWorkerCount, true)
	return pool, nil
}

//AssignTask распределяет работу между несколькими исполнителями
//Возвращает флаг таймаута поиска исполнителя и ошибку
func (p *Pool) AssignTask(request *Task, maxWorkerCount int, timeout time.Duration) (bool, error) {
	if maxWorkerCount < 0 {
		return false, fmt.Errorf("Negative value %v for maxWorkerCount is not allowed", maxWorkerCount)
	}
	p.setupVacancies(maxWorkerCount, false)

	vacancies := p.vacancies
	for {
		select {
		case p.taskQueue <- request:
			{
				//Задача назначена свободному исполнителю
				//Обновим вакансии и завершим работу менеджера
				//по обработке запроса
				vacancies = p.vacancies
				return false, nil
			}
		case <-p.vacancyChanged:
			{
				//Обновим список вакансий
				vacancies = p.vacancies
			}
		case <-vacancies:
			{
				//Свободных исполнителей нет, но есть вакансии
				//создадим нового исполнителя
				p.createWorker()
			}
		case <-time.After(timeout):
			{
				//Исполнитель не был назначен в течение отведённого времени
				return true, nil
			}
		}
	}
}

//createWorker создаёт нового исполнителя и регистрирует
//его в справочнике исполнителей workers
func (p *Pool) createWorker() {
	workerStopped := func(w *Worker) {
		p.vacanciesLock.Lock()
		delete(p.workers, w)
		if p.maxWorkerCount > len(p.workers) {
			p.vacancies <- signal
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
	go worker.Listen(p.taskQueue)
}

//setupVacancies управляет количеством текущих вакансий
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
		//Создадим в новом канале вакансий вакансии в количестве maxWorkerCount - currentWorkersCount
		for i := currentWorkersCount; i < maxWorkerCount; i++ {
			p.vacancies <- signal
		}
		i := 0
		//Пошлём "лишним" исполнителям сигнал остановки
		for w := range p.workers {
			if i >= currentWorkersCount-maxWorkerCount {
				break
			}
			select {
			case w.StopSignal <- signal:
			default:
				//Если данный исполнитель уже получил сигнал, то продолжим
			}
			i++
		}
	} else {
		p.vacancies = make(chan void, 1)
		p.vacancies <- signal
	}
	p.notifyForVacancies()
}

//notifyForVacancies уведомляет все ожидающие запросы
//об изменившихся вакансиях
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

//isInfiniteWorkersAllowed возвращает признак  отсутсвия ограничения
//количества одновременно работающих исполнителей
func (p *Pool) isInfiniteWorkersAllowed() bool {
	return p.maxWorkerCount == 0
}
