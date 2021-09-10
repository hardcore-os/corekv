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

package file

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
)

// SSTable 文件的内存封装
type SSTable struct {
	lock   *sync.RWMutex
	f      *MmapFile
	maxKey []byte
	minKey []byte
	indexs []byte
	fid    uint32
}

// OpenSStable 打开一个 sst文件
func OpenSStable(opt *Options) *SSTable {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{f: omf, fid: opt.FID, lock: &sync.RWMutex{}}
}

// Indexs 获取sst文件索引
func (ss *SSTable) Indexs() []byte {
	if len(ss.indexs) == 0 {
		ss.lock.RLock()
		bv := ss.f.Slice(0)
		ss.lock.RUnlock()
		m := make(map[string]interface{}, 0)
		json.Unmarshal(bv, &m)
		if idx, ok := m["idx"]; !ok {
			return []byte{}
		} else {
			dataStr, _ := idx.(string) // hello,0
			ss.indexs = []byte(dataStr)
			tmp := strings.Split(dataStr, ",")
			ss.maxKey = []byte(tmp[len(tmp)-1])
			ss.minKey = []byte(tmp[0])
		}
	}
	return ss.indexs
}

// MaxKey 当前最大的key
func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

// MinKey 当前最小的key
func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

// FID 获取fid
func (ss *SSTable) FID() uint32 {
	return ss.fid
}

// SaveSkipListToSSTable 将跳表序列化到sst文件中
// TODO 设计sst文件格式，并重写此处flush逻辑
func (ss *SSTable) SaveSkipListToSSTable(sl *utils.SkipList) error {
	iter := sl.NewIterator(&iterator.Options{})
	indexs, datas, idx := make([]string, 0), make([]string, 0), 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item().Entry()
		indexs = append(indexs, string(item.Key))
		indexs = append(indexs, fmt.Sprintf("%d", idx))
		datas = append(datas, string(item.Value))
		idx++
	}
	ssData := make(map[string]string, 0)
	ssData["idx"] = strings.Join(indexs, ",")
	ssData["data"] = strings.Join(datas, ",")
	bData, err := json.Marshal(ssData)
	utils.Err(err)
	ss.lock.Lock()
	// TODO 这里可能要在完善一下，想一下page的问题。
	fileData, _, err := ss.f.AllocateSlice(len(bData), 0)
	utils.Panic(err)
	copy(fileData, bData)
	ss.lock.Unlock()
	ss.indexs = []byte(ssData["idx"])
	return nil
}

// LoadData 加载数据块
func (ss *SSTable) LoadData() (blocks [][]byte, offsets []int) {
	ss.lock.RLock()
	fileData := ss.f.Slice(0)
	ss.lock.RUnlock()
	m := make(map[string]interface{}, 0)
	json.Unmarshal(fileData, &m)
	if data, ok := m["data"]; !ok {
		panic("sst data is nil")
	} else {
		// TODO 所有的数据都放在一个 block中
		dd := data.(string)
		blocks = append(blocks, []byte(dd))
		offsets = append(offsets, 0)
	}
	return blocks, offsets
}
