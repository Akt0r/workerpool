package workerpool

//Observer provides capabilities of pool activity monitoring
type Observer interface {
	WorkerCreated(p *Pool)
	WorkerDisposed(p *Pool)
	AllWorkersDisposed(p *Pool)
}
