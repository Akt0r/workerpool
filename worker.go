package workerpool

import (
	"fmt"
	"time"
)

//Task sample task type
type Task struct {
	Task       func() string
	TaskResult chan<- string
}

//Worker provides simple processing structure
type Worker struct {
	Name         string
	StopSignal   chan void
	StopCallback func(*Worker)
	listening    bool
}

//Listen perform requested task when idle
func (w *Worker) Listen(taskQueue <-chan *Task) {
	if w.listening {
		return
	}
	w.listening = true
	for {
		select {
		case <-w.StopSignal:
			{
				if w.StopCallback != nil {
					w.StopCallback(w)
				}
				return
			}
		case task := <-taskQueue:
			{
				if task.Task == nil || task.TaskResult == nil {
					continue
				}
				taskResult := task.Task()
				verboseTaskResult := fmt.Sprintf("%s complete work at %s %s", w.Name, time.Now().Format("15:04:05"), taskResult)
				task.TaskResult <- verboseTaskResult
				select {
				case <-w.StopSignal:
					{
						if w.StopCallback != nil {
							w.StopCallback(w)
						}
						return
					}
				default:
				}
			}
		}
	}
}
