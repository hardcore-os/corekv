package cache

import (
	"container/list"
	xxhash "github.com/cespare/xxhash/v2"
	"sync"
	"unsafe"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	door      *BloomFilter
	c         *cmSketch
	t         int32
	threshold int32
	data      map[uint64]*list.Element
}

type Options struct {
	lruPct uint8
}

// NewCache size 指的是要缓存的数据个数
func NewCache(size int) *Cache {
	//定义 window 部分缓存所占百分比，这里定义为1%
	const lruPct = 1
	//计算出来 widow 部分的容量
	lruSz := (lruPct * size) / 100

	if lruSz < 1 {
		lruSz = 1
	}

	// 计算 LFU 部分的缓存容量
	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))

	if slruSz < 1 {
		slruSz = 1
	}

	//LFU 分为两部分，stageOne 部分占比20%
	slruO := int(0.2 * float64(slruSz))

	if slruO < 1 {
		slruO = 1
	}

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		door: newFilter(size, 0.01), //布隆过滤器设置误差率为0.01
		c:    newCmSketch(int64(size)),
		data: data, //共用同一个 map 存储数据
	}

}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key, value interface{}) bool {
	if key == nil {
		return false
	}
	hashKey, conflict := c.keyToHash(key)

	item := storeItem{
		stage:    0,
		key:      hashKey,
		conflict: conflict,
		value:    value,
	}

	eitem, evicted := c.lru.add(item)

	if !evicted {
		return true
	}

	slruVict := c.slru.victim()
	if slruVict == nil {
		c.slru.add(eitem)
		return true
	}

	if !c.door.Allow(uint32(hashKey)) {
		return true
	}

	count0 := c.c.Estimate(eitem.key)
	count1 := c.c.Estimate(slruVict.key)

	if count0 < count1 {
		return true
	}

	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		c.c.Reset()
		c.door.reset()
		c.t = 0
	}

	if key == nil {
		return nil, false
	}

	keyHash, conflict := c.keyToHash(key)
	ref, ok := c.data[keyHash]

	if !ok {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	item := ref.Value.(*storeItem)

	if item.conflict != conflict {
		c.door.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}

	c.door.Allow(uint32(keyHash))
	c.c.Increment(keyHash)

	if item.stage == 0 {
		c.lru.get(ref)
	} else {
		c.slru.get(ref)
	}

	return item.value, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)

	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	delete(c.data, keyHash)
	return item.conflict, true
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + " | " + c.slru.String()
	return s
}
