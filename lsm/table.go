// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
)

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	ss := file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lm.opt.SSTableMaxSz)})
	t := &table{ss: ss, lm: lm, fid: utils.FID(tableName)}
	// 对builder来flush到此篇
	if builder != nil {
		if err := builder.flush(ss); err != nil {
			utils.Err(err)
			return nil
		}
	}
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return t
}

// Serach 从table中查找key
func (t *table) Serach(key []byte, maxVs *uint64) (entry *codec.Entry, err error) {
	// 获取索引
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&iterator.Options{})
	defer iter.Close()

	iter.Seek(key)
	if iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if codec.SameKey(key, iter.Item().Entry().Key) {
		if version := codec.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) indexKey() uint64 {
	return t.fid
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

// block function return a new block. Each block holds a ref and the byte
// slice stored in the block will be reused when the ref becomes zero. The
// caller should release the block by calling block.decrRef() on it.
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}

	// Read meta data related to block.
	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(codec.BytesToU32(b.data[readPos : readPos+4]))

	// Checksum length greater than block size could happen if the table was compressed and
	// it was opened with an incorrect compression algorithm (or the data was corrupted).
	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	// Read checksum and store it
	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]
	// Move back and read numEntries in the block.
	readPos -= 4
	numEntries := int(codec.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = codec.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	// Drop checksum and checksum length.
	// The checksum is calculated for actual data + entry index + index length
	b.data = b.data[:readPos+4]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	// Decrement the block ref if we could not insert it in the cache.
	t.lm.cache.blocks.Set(key, b)
	// We have added an OnReject func in our cache, which gets called in case the block is not
	// admitted to the cache. So, every block would be accounted for.
	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

type tableIterator struct {
	it       iterator.Item
	opt      *iterator.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *iterator.Options) iterator.Iterator {
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}
func (it *tableIterator) Next() {
}
func (it *tableIterator) Valid() bool {
	return it == nil
}
func (it *tableIterator) Rewind() {
}
func (it *tableIterator) Item() iterator.Item {
	return it.it
}
func (it *tableIterator) Close() error {
	return nil
}
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		// Offsets should never return false since we're iterating within the OffsetsLength.
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableIterator.Seek idx < 0 || idx > len(index.GetOffsets()"))
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		// The smallest key in our table is already strictly > key. We can return that.
		// This is like a SeekToFirst.
		it.seekHelper(0, key)
		return
	}

	// block[idx].smallest is > key.
	// Since idx>0, we know block[idx-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[idx-1] is strictly < key. In this case, we should go to the first
	//    element of block[idx].
	// 2) Some element in block[idx-1] is >= key. We should go to that element.
	it.seekHelper(idx-1, key)
	if it.err == io.EOF {
		// Case 1. Need to visit block[idx].
		if idx == len(it.t.ss.Indexs().Offsets) {
			// If idx == len(itr.t.blockIndex), then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		it.seekHelper(idx, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[idx-1].
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	*ko = *index.GetOffsets()[i]
	return true
}
