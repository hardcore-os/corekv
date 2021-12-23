package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	n   uint32 //offset
	buf []byte
}

const MaxNodeSize = int(unsafe.Sizeof(Element{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz)

	//if offset + MaxNode size > len(s.buf) overflow
	if int(offset) > len(s.buf)-MaxNodeSize {
		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}

		if growBy < sz {
			growBy = sz
		}

		newBuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}

	return offset - sz
}

//在arena里开辟一块空间，用以存放sl中的节点
//返回值为在arena中的offset
func (s *Arena) putNode(height int) uint32 {
	unusedSize := (defaultMaxLevel - height) * offsetSize

	l := uint32(MaxNodeSize - unusedSize + nodeAlign)

	n := s.allocate(l)

	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	l := v.EncodedSize()
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := s.allocate(keySz)
	buf := s.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))

	return offset
}

func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}

	return (*Element)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(s.buf[offset : offset+size])
	return
}

//用element在内存中的地址 - arena首字节的内存地址，得到在arena中的偏移量
func (s *Arena) getElementOffset(nd *Element) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&e.levels[h])
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
