package utils

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

type node struct {
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyOffset uint32 // Immutable. No need to lock to access key.
	keySize   uint16 // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint32
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, vo uint64) {
	atomic.StoreUint64(&n.value, vo)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

// getVs return ValueStruct stored in node
func (n *node) getVs(arena *Arena) ValueStruct {
	valOffset, valSize := n.getValueOffset()
	return arena.getVal(valOffset, valSize)
}

type Skiplist struct {
	height     int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	headOffset uint32
	ref        int32
	arena      *Arena
	OnClose    func()
}

// IncrRef increases the refcount
func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}
	if s.OnClose != nil {
		s.OnClose()
	}

	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Int31n(2) == 0 {
		h++
	}
	return h
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	keySize := len(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)

	node.height = uint16(height)
	node.value = val
	node.keySize = uint16(keySize)
	node.keyOffset = keyOffset

	return node
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valOffset)<<32 | uint64(valSize)
}

func decodeValue(val uint64) (uint32, uint32) {
	valSize := uint32(val)
	valOffset := uint32(val >> 32)

	return valOffset, valSize
}

func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	header := newNode(arena, nil, ValueStruct{}, maxHeight)
	headerOffset := arena.getNodeOffset(header)

	return &Skiplist{
		height:     1,
		headOffset: headerOffset,
		arena:      arena,
		ref:        1,
	}
}

func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := s.getHeight() - 1

	for {
		next := s.getNext(x, int(level))

		if next == nil {
			if level > 0 {
				level--
				continue
			}

			if !less {
				return nil, false
			}

			if x == s.getHead() {
				return nil, false
			}

			return x, false
		}

		cmp := CompareKeys(key, next.key(s.arena))

		switch cmp {
		case 1:
			x = next
			continue
		case 0:
			if allowEqual {
				return next, true
			}

			if !less {
				return s.getNext(next, 0), false
			}

			if level == 0 {
				if x == s.getHead() {
					return nil, false
				}
				return x, false
			}

			level--
		case -1:
			if level > 0 {
				level--
				continue
			}

			if less {
				if x == s.getHead() {
					return nil, false
				}
				return x, false
			}
			return next, false
		}
	}
}

func (s *Skiplist) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		prevNode := s.arena.getNode(before)
		next := prevNode.getNextOffset(level)
		nextNode := s.arena.getNode(next)

		if nextNode == nil {
			return before, next
		}

		cmp := CompareKeys(key, nextNode.key(s.arena))

		if cmp == 0 {
			return next, next
		} else if cmp == 1 {
			before = next
			continue
		} else {
			return before, next
		}
	}
}

func (s *Skiplist) Add(entry *Entry) {
	key, vs := entry.Key, ValueStruct{
		Meta:      entry.Meta,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
		Version:   entry.Version,
	}

	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32
	height := s.getHeight()
	prev[s.height] = s.headOffset

	for i := height - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], int(i))
		if prev[i] == next[i] {
			valOffset := s.arena.putVal(vs)
			encValue := encodeValue(valOffset, vs.EncodedSize())
			prevNode := s.arena.getNode(prev[i])
			prevNode.setValue(s.arena, encValue)
			return
		}
	}

	growth := s.randomHeight()
	listHeight := s.getHeight()
	for growth > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(growth)) {
			break
		}
		listHeight = s.getHeight()
	}

	newNode := newNode(s.arena, key, vs, growth)
	for i := 0; i < growth; i++ {
		for {
			preNode := s.arena.getNode(prev[i])
			if preNode == nil {
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
			}

			newNode.tower[i] = next[i]
			preNode = s.arena.getNode(prev[i])
			if preNode.casNextOffset(i, next[i], s.arena.getNodeOffset(newNode)) {
				break
			}

			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				valOffset := s.arena.putVal(vs)
				encValue := encodeValue(valOffset, vs.EncodedSize())
				prevNode := s.arena.getNode(prev[i])
				prevNode.setValue(s.arena, encValue)
				return
			}
		}
	}
}

func (s *Skiplist) Search(key []byte) ValueStruct {
	target, _ := s.findNear(key, false, true)
	if target == nil {
		return ValueStruct{}
	}

	tk := s.arena.getKey(target.keyOffset, target.keySize)
	if !SameKey(key, tk) {
		return ValueStruct{}
	}
	vo, vs := target.getValueOffset()
	return s.arena.getVal(vo, vs)
}

func (s *Skiplist) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = s.getNext(next, level)
			if next != nil {
				key := next.key(s.arena)
				vs := next.getVs(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// align
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// plot
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type SkipListIterator struct {
	list *Skiplist
	n    *node
}

func (s *Skiplist) NewSkipListIterator() *SkipListIterator {
	s.IncrRef()
	return &SkipListIterator{
		list: s,
	}
}

func (s *SkipListIterator) Rewind() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

func (s *SkipListIterator) Valid() bool {
	return s.n != nil
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

func (s *SkipListIterator) Value() ValueStruct {
	return s.list.arena.getVal(s.n.getValueOffset())
}
