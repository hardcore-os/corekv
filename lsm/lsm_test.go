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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/hardcore-os/corekv/utils"
	"github.com/stretchr/testify/assert"
)

var (
	// 初始化opt

	opt = &Options{
		WorkDir:             "../work_test",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       2,
	}
)

// 对level 管理器的功能测试
func TestBase(t *testing.T) {
	clearDir()
	test := func() {
		lsm := buildLSM()
		// 基准chess
		baseTest(t, lsm, 128)
	}
	// 运行N次测试多个sst的影响
	runTest(test, 2)
}

// TestRecovery _
func TestRecovery(t *testing.T) {
	test := func() {
		lsm := buildLSM()
		// 测试正确性
		baseTest(t, lsm, 128)
		// 来一个新的wal文件
		lsm.Set(buildEntry())
	}
	// 允许两次就能实现恢复
	runTest(test, 1)
}

// 对level 管理器的功能测试
func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	test := func() {
		baseTest(t, lsm, 100)
	}
	// 运行N次测试多个sst的影响
	runTest(test, 10)
}

func buildLSM() *LSM {
	// init DB Basic Test
	lsm := NewLSM(opt)
	return lsm
}
func buildEntry() *utils.Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", randStr(16), "12345678"))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &utils.Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}
func baseTest(t *testing.T, lsm *LSM, n int) {
	// 用来跟踪调试的
	e := &utils.Entry{
		Key:       []byte("CRTSmI4xYMrGSBtL12345678"),
		Value:     []byte("hImkq95pkCRARFlUoQpCYUiNWYV9lkOd9xiUs0XtFNdOZe5siJVcxjc6j3E5LUng"),
		ExpiresAt: 0,
	}

	lsm.Set(e)
	for i := 1; i < n; i++ {
		lsm.Set(buildEntry())
	}
	// 从levels中进行GET
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	assert.Equal(t, e.Value, v.Value)
}

func runTest(test func(), n int) {
	for i := 0; i < n; i++ {
		test()
	}
}

func randStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
