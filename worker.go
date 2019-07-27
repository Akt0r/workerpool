package workerpool

//Task sample task type
type Task struct {
	Task   func() TaskResult
	Result chan<- TaskResult
}

//TaskResult contains info about task completion
type TaskResult interface {
}

//Worker provides simple processing structure
type Worker struct {
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
				if task.Task == nil || task.Result == nil {
					continue
				}
				taskResult := task.Task()
				task.Result <- taskResult
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
