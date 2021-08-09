package lsm

import "github.com/hardcore-os/corekv/utils"

type cache struct {
	indexs *utils.CoreMap // key fid， value tableBuffer
	blocks *utils.CoreMap // key cacheID_blockOffset  value block []byte
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
	return &cache{indexs: utils.NewMap(), blocks: utils.NewMap()}
}

func (c *cache) addIndex(fid int64, t *table) {
	c.indexs.Set(fid, t)
}
