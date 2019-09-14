package workerpool

//WithConcurrencyLimitOf setups pool workers capacity thus
//bounding concurrency rate. Zero have special meaning of
//having no upper bound. Default is zero.
func WithConcurrencyLimitOf(lim int) func(*Pool) error {
	return func(p *Pool) error {
		return p.SetConcurrencyLimit(lim)
	}
}
