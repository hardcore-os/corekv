package corekv

type Options struct {
	ValueThreshold int64
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{}
	opt.ValueThreshold = 1024
	return opt
}
