package lsm

import "github.com/hardcorexs/corekv/file"

type levelManager struct {
	opt      *Options
	cache    *cache
	manifest *file.Manifest
	levels   []*levelHandler
}

type levelHandler struct {
	tables []*table
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
