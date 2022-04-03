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

package utils

import (
	"reflect"
	"sync"

	"github.com/pkg/errors"
)

// CoreMap _
type CoreMap struct {
	m sync.Map
}

// NewMap _
func NewMap() *CoreMap {
	return &CoreMap{m: sync.Map{}}
}

// Get _
func (c *CoreMap) Get(key interface{}) (interface{}, bool) {
	hashKey := c.keyToHash(key)
	return c.m.Load(hashKey)
}

// Set _
func (c *CoreMap) Set(key, value interface{}) {
	hashKey := c.keyToHash(key)
	c.m.Store(hashKey, value)
}

// Del _
func (c *CoreMap) Del(key interface{}) {
	hashKey := c.keyToHash(key)
	c.m.Delete(hashKey)
}

// Range _
func (c *CoreMap) Range(f func(key, value interface{}) bool) {
	c.m.Range(f)
}

func (c *CoreMap) keyToHash(key interface{}) uint64 {
	if key == nil {
		return 0
	}
	switch k := key.(type) {
	case []byte:
		return MemHash(k)
	case uint32:
		return uint64(k)
	case string:
		return MemHashString(k)
	case uint64:
		return k
	case byte:
		return uint64(k)
	case int:
		return uint64(k)
	case int32:
		return uint64(k)

	case int64:
		return uint64(k)
	default:
		CondPanic(true, errors.Errorf("Key:[%+v] type not supported", reflect.TypeOf(k)))
	}
	return 0
}
