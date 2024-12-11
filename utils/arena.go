/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueOffset uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	// 始终在64位边界上对结点进行对齐
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

// Arena should be lock-free（无锁）.
type Arena struct {
	n          uint32 // 表示当前Arena中已经使用的结点数量。
	shouldGrow bool   // 指示Arena是否应该在空间不足的情况下自动增长。
	buf        []byte // Arena的主要内存存储区，用于存储实际的字节数据。
}

// newArena returns a new arena.
func newArena(n int64) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz) // 原子性地对 Arena.n 加上 sz
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))
		return offset - sz
	}

	// We are keeping extra bytes in the end so that the checkptr doesn't fail. We apply some
	// intelligence to reduce the size of the node by only keeping towers upto valid height and not
	// maxHeight. This reduces the node's size, but checkptr doesn't know about its reduced size.
	// checkptr tries to verify that the node of size MaxNodeSize resides on a single heap
	// allocation which causes this error: checkptr:converted pointer straddles multiple allocations
	if int(offset) > len(s.buf)-MaxNodeSize { // 表示当前的缓冲区已经不足以容纳一个完整的节点。
		growBy := uint32(len(s.buf)) // growBy表示要扩容的大小
		if growBy > 1<<30 {          //设定最大值，防止在扩容时分配过大的内存
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}
		newBuf := make([]byte, len(s.buf)+int(growBy))
		// AssertTrue函数确保将原来的s.buf数据复制到新的newBuf时，复制的字节数与原切片s.buf的长度相同。
		// copy函数返回实际复制的字节数。
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
		// fmt.Print(len(s.buf), " ")
	}
	return offset - sz // 返回的是当前新分配节点的起始偏移量
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
// 为一个结点分配内存，并确保该结点的内存地址是对齐的
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize // offsetSize是一层tower的大小

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign) // 计算对齐后的分配大小。+ nodeAlign是为了增加内存对齐的功能后不会出错
	n := s.allocate(l)

	// Return the aligned offset.
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign) // 对齐粒度为MEM_ALIGNMENT字节时，((size) + MEM_ALIGNMENT - 1) & ~(MEM_ALIGNMENT-1)) 为向上取整
	return m
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset : offset+l])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

// getKey returns byte slice at offset.
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

// getVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0 //返回空指针
	}
	//implement me here！！！
	//获取某个节点,在 arena 当中的偏移量
	//unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
