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
	coreCache "github.com/hardcore-os/corekv/utils/cache"
)

type cache struct {
	indexs *coreCache.Cache // key fid， value table
	blocks *coreCache.Cache // key fid_blockOffset  value block []byte
}

type blockBuffer struct {
	b []byte
}

const defaultCacheSize = 1024

// close
func (c *cache) close() error {
	return nil
}

// newCache
func newCache(opt *Options) *cache {
	return &cache{indexs: coreCache.NewCache(defaultCacheSize), blocks: coreCache.NewCache(defaultCacheSize)}
}

// TODO fid 使用字符串是不是会有性能损耗
func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
