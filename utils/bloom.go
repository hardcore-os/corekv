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

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	//Implement me here!!!
	//在这里实现判断一个数据是否在bloom过滤器中
	//思路大概是经过K个Hash函数计算，判读对应位置是否被标记为1
	return true
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	//Implement me here!!!
	//阅读bloom论文实现，并在这里编写公式
	//传入参数numEntries是bloom中存储的数据个数，fp是false positive假阳性率
	return 0
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	//Implement me here!!!
	//在这里实现将一个Key值放入到bloom过滤器中
	return make([]byte, 0)
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	//Implement me here!!!
	//在这里实现高效的HashFunction
	return 0
}
