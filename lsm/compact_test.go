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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 对level 管理器的功能测试
func TestCompact(t *testing.T) {
	clearDir()
	rand.Seed(time.Now().Unix())
	lsm := buildLSM()
	test := func() {
		lsm.StartCompacter()
		baseTest(t, lsm, 4)
		time.Sleep(10 * time.Second)
	}
	// 运行N次测试多个sst的影响
	runTest(test, 1)
	// TODO 更新 manifest 文件有问题，但是已经真正的发生了合并
	// 为什么合并后 sst文件反而变小了？
	assert.True(t, len(lsm.levels.manifestFile.GetManifest().Levels[6].Tables) != 0)
}
