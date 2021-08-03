package lsm

import "github.com/hardcore-os/corekv/utils"

type cache struct {
	indexs utils.CoreMap // key fid， value tableBuffer
	blocks utils.CoreMap // key cacheID_blockOffset  value block []byte
}
type tableBuffer struct {
	t       *table
	cacheID int64
}
type blockBuffer struct {
	b []byte
}

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{}
}
