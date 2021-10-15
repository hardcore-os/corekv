// Copyright 2021 hardcore Project Authors
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
	"fmt"
	"os"
	"testing"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/stretchr/testify/assert"
)

// 对level 管理器的功能测试
func TestLevels(t *testing.T) {
	entrys := []*codec.Entry{
		{Key: []byte("hello0_12345678"), Value: []byte("world0"), ExpiresAt: uint64(0)},
		{Key: []byte("hello1_12345678"), Value: []byte("world1"), ExpiresAt: uint64(0)},
		{Key: []byte("hello2_12345678"), Value: []byte("world2"), ExpiresAt: uint64(0)},
		{Key: []byte("hello3_12345678"), Value: []byte("world3"), ExpiresAt: uint64(0)},
		{Key: []byte("hello4_12345678"), Value: []byte("world4"), ExpiresAt: uint64(0)},
		{Key: []byte("hello5_12345678"), Value: []byte("world5"), ExpiresAt: uint64(0)},
		{Key: []byte("hello6_12345678"), Value: []byte("world6"), ExpiresAt: uint64(0)},
		{Key: []byte("hello7_12345678"), Value: []byte("world7"), ExpiresAt: uint64(0)},
	}
	// 初始化opt
	opt := &Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       283,
		MemTableSize:       1024,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
	}
	levelLive := func() {
		// 初始化
		levels := newLevelManager(opt)
		defer func() { _ = levels.close() }()
		fileName := fmt.Sprintf("%s/%s", opt.WorkDir, "000001.mem")
		// 构建内存表
		imm := &memTable{
			wal: file.OpenWalFile(&file.Options{FileName: fileName, Dir: opt.WorkDir, Flag: os.O_CREATE | os.O_RDWR, MaxSz: int(opt.SSTableMaxSz)}),
			sl:  utils.NewSkipList(),
		}
		for _, entry := range entrys {
			imm.set(entry)
		}
		// 测试 flush
		assert.Nil(t, levels.flush(imm))
		// 从levels中进行GET
		v, err := levels.Get([]byte("hello7_12345678"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("world7"), v.Value)
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)
		// 关闭levels
		assert.Nil(t, levels.close())
	}
	// 运行N次测试多个sst的影响
	for i := 0; i < 10; i++ {
		levelLive()
	}
}

// 对level管理器的性能测试
