package corekv

import "github.com/hardcore-os/corekv/utils"

type Options struct {
	ValueThreshold int64
}

// NewDefaultOptions 返回默认的options
func NewDefaultOptions() *Options {
	opt := &Options{}
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}
