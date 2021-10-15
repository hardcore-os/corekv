package lsm

import (
	"bytes"
	"sort"
	"sync"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

type levelManager struct {
	maxFid       uint64
	opt          *Options
	cache        *cache
	manifestFile *file.ManifestFile
	levels       []*levelHandler
}

type levelHandler struct {
	sync.RWMutex
	levelNum int
	tables   []*table
}

func (lh *levelHandler) close() error {
	return nil
}
func (lh *levelHandler) add(t *table) {
	lh.tables = append(lh.tables, t)
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	// 如果是第0层文件则进行特殊处理
	if lh.levelNum == 0 {
		// TODO：logic...
		// 获取可能存在key的sst
		return lh.searchL0SST(key)
	} else {
		// TODO：logic...
		return lh.searchLNSST(key)
	}
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// Sort tables by keys.
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].ss.MinKey(), lh.tables[j].ss.MinKey()) < 0
		})
	}
}

func (lh *levelHandler) searchL0SST(key []byte) (*codec.Entry, error) {
	var version uint64
	for _, table := range lh.tables {
		if entry, err := table.Serach(key, &version); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) searchLNSST(key []byte) (*codec.Entry, error) {
	table := lh.getTable(key)
	var version uint64
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if entry, err := table.Serach(key, &version); err == nil {
		return entry, nil
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) getTable(key []byte) *table {
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if bytes.Compare(key, lh.tables[i].ss.MinKey()) > -1 &&
			bytes.Compare(key, lh.tables[i].ss.MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
}
func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestFile.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	// L0层查询
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7层查询
	for level := 1; level < utils.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}
func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	// 读取manifest文件构建管理器
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	lm.build()
	return lm
}
func (lm *levelManager) loadCache() {
	lm.cache = newCache(lm.opt)
	// 添加 idx cache
	for _, level := range lm.levels {
		for _, table := range level.tables {
			lm.cache.addIndex(table.ss.FID(), table)
		}
	}
}
func (lm *levelManager) loadManifest() (err error) {
	lm.manifestFile, err = file.OpenManifestFile(&file.Options{Dir: lm.opt.WorkDir})
	return err
}
func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, utils.MaxLevelNum)
	for i := 0; i < utils.MaxLevelNum; i++ {
		lm.levels = append(lm.levels, &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
		})
	}

	manifest := lm.manifestFile.GetManifest()
	// 对比manifest 文件的正确性
	if err := lm.manifestFile.RevertToManifest(utils.LoadIDMap(lm.opt.WorkDir)); err != nil {
		return err
	}
	var maxFid uint64
	for fID, tableInfo := range manifest.Tables {
		fileName := utils.FileNameSSTable(lm.opt.WorkDir, fID)
		if fID > maxFid {
			maxFid = fID
		}
		t := openTable(lm, fileName, nil)
		lm.levels[tableInfo.Level].tables = append(lm.levels[tableInfo.Level].tables, t)
	}
	// 对每一层进行排序
	for i := 0; i < utils.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// 得到最大的fid值
	lm.maxFid = maxFid
	// 逐一加载sstable 的index block 构建cache
	lm.loadCache()
	return nil
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) error {
	// TODO LAB
	return nil
}
