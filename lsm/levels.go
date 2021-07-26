package lsm

import (
	"github.com/hardcore-os/corekv/codec"
	"github.com/hardcore-os/corekv/file"
)

type levelManager struct {
	opt      *Options
	cache    *cache
	manifest *file.Manifest
	levels   []*levelHandler
}

type levelHandler struct {
	tables []*table
}

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) Get(key []byte) *codec.Entry {
	return nil
}
func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifest.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	// 读取manifest文件构建管理器
	lm.loadManifest()
	lm.build()
	return lm
}
func (lm *levelManager) loadCache() {

}
func (lm *levelManager) loadManifest() {
	lm.manifest = file.OpenManifest(&file.Options{})
}
func (lm *levelManager) build() {
	// 如果manifest文件是空的 则进行初始化
	lm.levels = make([]*levelHandler, 0)
	lm.levels = append(lm.levels, &levelHandler{tables: []*table{openTable(lm.opt)}})
	// 逐一加载sstable 的index block 构建cache
	lm.loadCache()
}

func (lm *levelManager) flush(immutable *memTable) error {
	// 向L0层flush一个sstable
	return nil
}

func (lm *levelManager) Get(key []byte) *codec.Entry {
	var entry *codec.Entry
	// L0层查询
	if entry = lm.levels[0].Get(key); entry != nil {
		return entry
	}
	// L1-7层查询
	for level := 1; level < 8; level++ {
		ld := lm.levels[level]
		if entry = ld.Get(key); entry != nil {
			return entry
		}
	}
	return entry
}
