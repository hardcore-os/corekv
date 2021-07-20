package api

type Options struct {
}

type IteratorOptions struct {
	Prefix []byte
	IsAsc  bool
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	return &Options{}
}
