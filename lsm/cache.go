package lsm

type cache struct {
}

// Close
func (c *cache) close() error {
	return nil
}

// NewCache
func newCache(opt *Options) *cache {
	return &cache{}
}
