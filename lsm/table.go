package lsm

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

type table struct {
	ss   *file.SSTable
	lm   *levelManager
	fid  uint32
	idxs []byte
}

func openTable(lm *levelManager, tableName string) *table {
	t := &table{ss: file.OpenSStable(&file.Options{Name: tableName, Dir: lm.opt.WorkDir})}
	// 加载ss文件 索引
	t.idxs = t.ss.Indexs()
	// 反引用 level manager
	t.lm = lm
	// 获取fid
	j := 0
	for i := range tableName {
		if tableName[i] != '0'-0 {
			break
		}
		j++
	}
	fidStr := strings.Split(tableName[j:], ".")[0]
	fidU64, err := strconv.ParseUint(fidStr, 10, 32)
	utils.Panic(err)
	t.fid = uint32(fidU64)
	return t
}

// Serach 从table中查找key
func (t *table) Serach(key []byte) (entry *codec.Entry, err error) {
	// TODO 二分法在idx中查找
	keyStr := string(key)
	idxStr := string(t.idxs)
	idxx := strings.Split(idxStr, ",")
	idx := -1
	for i := 0; i < len(idxx); i += 2 {
		if keyStr == idxx[i] {
			idx, err = strconv.Atoi(idxx[i+1])
			utils.Panic(err)
		}
	}
	if idx == -1 {
		return nil, utils.ErrKeyNotFound
	}
	// 从缓存中查询数据, 当前
	if block, ok := t.lm.cache.blocks.Get(fmt.Sprintf("%d-%d", t.fid, 0)); ok {
		data, _ := block.([]byte)
		return t.getEntry(key, data, idx)
	}
	// 如果数据没有则从磁盘中加载后查询
	var block []byte
	blocks, offsets := t.ss.LoadData()
	if len(blocks) > 0 {
		block = blocks[0]
		t.lm.cache.blocks.Set(fmt.Sprintf("%d-%d", t.fid, offsets[0]), blocks[0])
	}
	return t.getEntry(key, block, idx)
}
func (t *table) getEntry(key, block []byte, idx int) (entry *codec.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &codec.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}
