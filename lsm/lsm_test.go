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
	"testing"

	"github.com/hardcore-os/corekv/utils"
	"github.com/stretchr/testify/assert"
)

var (
	// case
	entrys = []*utils.Entry{
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
	opt = &Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       283,
		MemTableSize:       224,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
	}
)

// 对level 管理器的功能测试
func TestLSM(t *testing.T) {
	levelLive := func() {
		// 初始化
		lsm := NewLSM(opt)
		for _, entry := range entrys {
			lsm.Set(entry)
		}
		// 测试 flush
		assert.Nil(t, lsm.levels.flush(lsm.memTable))

		// 从levels中进行GET
		v, err := lsm.Get([]byte("hello7_12345678"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("world7"), v.Value)
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)

		// 测试Recovery
		// 再写一次
		lsm.memTable = lsm.NewMemtable()
		for _, entry := range entrys {
			lsm.Set(entry)
		}
		// 强制recovery 相当于数据库重启，工作目录中wal文件不会清空

		imm, imms := lsm.recovery()
		assert.NotEmpty(t, imm)
		assert.Equal(t, len(imms) != 0, true)
		for _, mem := range imms {
			e, _ := mem.Get([]byte("hello7_12345678"))
			assert.Equal(t, []byte("world7"), e.Value)
		}
	}
	// 运行N次测试多个sst的影响
	for i := 0; i < 2; i++ {
		levelLive()
	}
}
