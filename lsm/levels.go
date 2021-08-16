package lsm

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

type levelManager struct {
	maxFid   uint32
	opt      *Options
	cache    *cache
	manifest *file.Manifest
	levels   []*levelHandler
}

type levelHandler struct {
	levelNum int
	tables   []*table
}

func (lh *levelHandler) close() error {
	return nil
}
func (lh *levelHandler) add(sst *file.SSTable) {
	lh.tables = append(lh.tables, &table{
		ss:   sst,
		idxs: sst.Indexs(),
	})
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

func (lh *levelHandler) searchL0SST(key []byte) (*codec.Entry, error) {
	for _, table := range lh.tables {
		if entry, err := table.Serach(key); err == nil {
			return entry, nil
		}
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) searchLNSST(key []byte) (*codec.Entry, error) {
	table := lh.getTable(key)
	if entry, err := table.Serach(key); err == nil {
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
	return entry, nil
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
	lm.cache = newCache(lm.opt)
	// 添加 idx cache
	for _, level := range lm.levels {
		for _, table := range level.tables {
			lm.cache.addIndex(table.ss.FID(), table)
		}
	}
}
func (lm *levelManager) loadManifest() {
	//TODO: 从 current 文件中拿到当前manifest文件名
	lm.manifest = file.OpenManifest(&file.Options{Name: "MANIFEST", Dir: lm.opt.WorkDir})
}
func (lm *levelManager) build() {
	// 如果manifest文件是空的 则进行初始化
	lm.levels = make([]*levelHandler, utils.MaxLevelNum)
	tables := lm.manifest.Tables()
	var maxFid uint32
	for num := 0; num < utils.MaxLevelNum; num++ {
		lm.levels[num] = &levelHandler{levelNum: num}
		lm.levels[num].tables = make([]*table, len(tables[num]))
		for i := range tables[num] {
			ot := openTable(lm, tables[num][i].SSTName)
			lm.levels[num].tables[i] = ot
			if ot.fid > maxFid {
				maxFid = ot.fid
			}
		}
	}
	// 得到最大的fid值
	lm.maxFid = maxFid
	// 逐一加载sstable 的index block 构建cache
	lm.loadCache()
}

// 向L0层flush一个sstable
func (lm *levelManager) flush(immutable *memTable) error {
	// 分配一个fid
	newFid := atomic.AddUint32(&lm.maxFid, 1)
	sstName := fmt.Sprintf("%05d.sst", newFid)
	// 创建一个sstable对象
	sst := file.OpenSStable(&file.Options{Name: sstName, Dir: lm.opt.WorkDir})
	// 跳表中的数据转化为sst文件
	if err := sst.SaveSkipListToSSTable(immutable.sl); err != nil {
		return err
	}
	// 更新manifest文件
	lm.levels[0].add(sst)
	return lm.manifest.AppendSST(0, &file.Cell{
		SSTName: sstName,
	})
}
