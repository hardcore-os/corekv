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
	"encoding/binary"
	"time"
)

type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
}

// value只持久化具体的value值和过期时间
func (e *ValueStruct) EncodedSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// DecodeValue
func (e *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	e.ExpiresAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

//对value进行编码，并将编码后的字节写入byte
//这里将过期时间和value的值一起编码
func (e *ValueStruct) EncodeValue(b []byte) uint32 {
	sz := binary.PutUvarint(b[:], e.ExpiresAt)
	n := copy(b[sz:], e.Value)
	return uint32(sz + n)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

//最外层写入的结构体
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}
