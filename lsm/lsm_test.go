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
	"os"
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
		SSTableMaxSz:       1024,
		MemTableSize:       1024,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
	}
)

// 对level 管理器的功能测试
func TestFlushBase(t *testing.T) {
	createDir(t, opt.WorkDir)
	lsm := buildCase()
	defer cleanDir(t, opt.WorkDir)
	test := func() {
		// 测试 flush
		assert.Nil(t, lsm.levels.flush(lsm.memTable))
		// 基准chess
		baseTest(t, lsm)

	}
	// 运行N次测试多个sst的影响
	runTest(test, 2)
}

// TestRecovery _
func TestRecoveryBase(t *testing.T) {
	createDir(t, opt.WorkDir)
	defer cleanDir(t, opt.WorkDir)
	buildCase()

	test := func() {
		// 丢弃整个LSM结构模拟数据库崩溃恢复
		lsm := NewLSM(opt)
		// 测试正确性
		baseTest(t, lsm)
	}
	runTest(test, 1)
}

func buildCase() *LSM {
	// init DB Basic Test
	lsm := NewLSM(opt)
	for _, entry := range entrys {
		lsm.Set(entry)
	}
	return lsm
}
func baseTest(t *testing.T, lsm *LSM) {
	// 从levels中进行GET
	v, err := lsm.Get([]byte("hello7_12345678"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world7"), v.Value)
	t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)
}

func runTest(test func(), n int) {
	for i := 0; i < n; i++ {
		test()
	}
}

func cleanDir(t *testing.T, dir string) {
	assert.Nil(t, os.RemoveAll(dir))
}

func createDir(t *testing.T, dir string) {
	assert.Nil(t, os.Mkdir(dir, 0755))
}
