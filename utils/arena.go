package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

type Arena struct {
	n   uint32 //offset
	buf []byte
}

const MaxNodeSize = int(unsafe.Sizeof(Element{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint32(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) allocate(sz uint32) uint32 {
	//implement me here！！！
	// 在 arena 中分配指定大小的内存空间
	offset := atomic.AddUint32(&s.n, sz) //返回加完之后的值
	//这里需要判断剩余的空间是否足够,这里的处理思路不是看是否有剩下的空间
	//而是看剩下的空间是否足以放下一个node
	// fmt.Print(uint32(len(s.buf)), uint32(offset), "\n")
	if int32(len(s.buf))-int32(offset) < int32(MaxNodeSize) { //这里比较时不要转化为uint,这样回吧负数错误转化为正数
		//空间不够,就扩容,首先试一下double
		size := uint32(len(s.buf))
		if size > 1<<30 { //超过一个G就限制
			size = 1 << 30
		}
		if int32(size)+int32(len(s.buf))-int32(offset) < int32(MaxNodeSize) {
			size = uint32(MaxNodeSize) + uint32(offset) - uint32(len(s.buf)) //不够的话就直接给这么多
		}
		newBuf := make([]byte, len(s.buf)+int(size))
		//确保正确的复制了
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
		// fmt.Print(len(s.buf), " ")
	}
	return offset - sz //表示从这里开始写
}

//在arena里开辟一块空间，用以存放sl中的节点
//返回值为在arena中的offset
func (s *Arena) putNode(height int) uint32 {
	//implement me here！！！
	// 这里的 node 要保存 value 、key 和 next 指针值
	// 所以要计算清楚需要申请多大的内存空间,这里表示要申请高度为height的node
	unusedSize := (defaultMaxLevel - height) * offsetSize
	l := MaxNodeSize - unusedSize + nodeAlign                                  // 计算出实际需要申请的空间,但是需要优化一步内存对齐
	offset := s.allocate((uint32(l) + uint32(nodeAlign)) &^ uint32(nodeAlign)) //做一步内存对齐的操作,对于一块m大小的内存空间,
	//进行align对齐的操作是(m+align)&^(align)
	return offset
}

//为什么在putKey和putValue的时候不做内存对齐的操作呢
func (s *Arena) putVal(v ValueStruct) uint32 {
	//implement me here！！！
	//将 Value 值存储到 arena 当中
	// 并且将指针返回,返回的指针值应被存储在 Node 节点中
	size := v.EncodedSize()    //获取压缩后value的大小
	offset := s.allocate(size) //分配空间
	v.EncodeValue(s.buf[offset : offset+size])
	return offset
}

func (s *Arena) putKey(key []byte) uint32 {
	//implement me here！！！
	//将  Key 值存储到 arena 当中
	// 并且将指针返回,返回的指针值应被存储在 Node 节点中
	offset := s.allocate(uint32(len(key)))
	// fmt.Print(offset, " ")
	copy(s.buf[offset:], key)
	return offset
}

//修改判断条件
func (s *Arena) getElement(offset uint32) *Element {
	if offset <= 0 { //offset为0处表示空指针,因为这样就可以让elemet的level[i] = 0表示空了
		return nil
	}
	return (*Element)(unsafe.Pointer(&s.buf[offset])) //越界会报painc
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
		return 0 //返回空指针
	}
	//implement me here！！！
	//获取某个节点,在 arena 当中的偏移量
	//unsafe.Pointer等价于void*,uintptr可以专门把void*的对于地址转化为数值型变量
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	//implement me here！！！
	// 这个方法用来计算节点在h 层数下的 next 节点
	return e.levels[h]//这里Add操作时加了锁的,所以我认为这里不需要使用原子操作
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
