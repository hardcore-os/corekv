package utils

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

type SkipListIterator struct {
	it *Element
	sl *SkipList
}

// NewIterator 跳表迭代器
func (sl *SkipList) NewIterator(opt *Options) Iterator {
	iter := &SkipListIterator{
		it: sl.header,
		sl: sl,
	}
	return iter
}
func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())

	return &SkipList{
		header: &Element{
			levels: make([]*Element, defaultMaxLevel),
			entry:  nil,
			score:  0,
		},
		rand:     rand.New(source),
		maxLevel: defaultMaxLevel,
		length:   0,
	}
}

func (iter *SkipListIterator) Next() {
	iter.it = iter.it.levels[0]
}
func (iter *SkipListIterator) Valid() bool {
	return iter.it != nil
}
func (iter *SkipListIterator) Rewind() {
	iter.it = iter.sl.header.levels[0]
}
func (iter *SkipListIterator) Item() Item {
	return iter.it
}
func (iter *SkipListIterator) Close() error {
	return nil
}

func (iter *SkipListIterator) Seek(key []byte) {
}

type Element struct {
	levels []*Element
	entry  *Entry
	score  float64
}

func newElement(score float64, entry *Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *Entry {
	return elem.entry
}

func (list *SkipList) Add(data *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	score := list.calcScore(data.Key)
	var elem *Element

	max := len(list.header.levels)
	prevElem := list.header

	var prevElemHeaders [defaultMaxLevel]*Element

	for i := max - 1; i >= 0; {
		//keep visit path here
		prevElemHeaders[i] = prevElem

		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					elem = next
					elem.entry = data
					list.size += elem.Entry().Size() - data.Size()
					return nil
				}

				//find the insert position
				break
			}

			//just like linked-list next
			prevElem = next
			prevElemHeaders[i] = prevElem
		}

		topLevel := prevElem.levels[i]

		//to skip same prevHeader's next and fill next elem into temp element
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}

	level := list.randLevel()

	elem = newElement(score, data, level)
	//to add elem to the skiplist
	for i := 0; i < level; i++ {
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = elem
	}
	list.size += data.Size()
	list.length++
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()
	if list.length == 0 {
		return nil
	}
	// 计算分值
	score := list.calcScore(key)

	prevElem := list.header
	i := len(list.header.levels) - 1
	var prevElemHeaders [defaultMaxLevel]*Element
	for i >= 0 {
		prevElemHeaders[i] = prevElem
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return next.Entry()
				}
				// key小于nextkey，证明在这一层没找到，跳出这个循环
				break
			}

			prevElem = next
		}

		topLevel := prevElem.levels[i]
		// 保存这一层的搜索到的节点，如果下一层的下个节点和这一层的下个节点一样，那么继续到下一层。
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}
	return
}

/*func (list *SkipList) Remove(key []byte) error {
	score := list.calcScore(key)

	max := len(list.header.levels)
	prevElem := list.header

	var prevElemHeaders [defaultMaxLevel]*Element
	var elem *Element

	for i := max - 1; i >= 0; {
		//keep visit path here
		prevElemHeaders[i] = prevElem

		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					elem = next
				}
				break
			}

			//just like linked-list next
			prevElem = next
			prevElemHeaders[i] = prevElem
		}

		topLevel := prevElem.levels[i]

		//to skip same prevHeader's next and fill next elem into temp element
		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {
			prevElemHeaders[i] = prevElem
		}
	}

	if elem == nil {
		return nil
	}

	prevTopLevel := len(elem.levels)
	for i := 0; i < prevTopLevel; i++ {
		prevElemHeaders[i].levels[i] = elem.levels[i]
	}

	list.length--
	return nil
}*/

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	if list.maxLevel <= 1 {
		return 1
	}
	i := 1
	// 使用redis的算法，层数越高，生成高层数的概率越小
	for {
		if (rand.Intn(32767) & 0xFFFF) < int(math.Round(defaultSkipListP*0xFFFF)) {
			i += 1
		}
		break
	}
	//for ; i < list.maxLevel; i++ {
	//	if RandN(1000)%2 == 0 {
	//		return i
	//	}
	//}
	return i
}

func (list *SkipList) Size() int64 {
	return list.size
}

type SkipListIter struct {
	header *Element
	elem   *Element
	lock   sync.RWMutex
}

func (list *SkipList) NewSkipListIterator() Iterator {
	return &SkipListIter{elem: list.header.levels[0], header: list.header}
}

func (iter *SkipListIter) Next() {
	iter.lock.RLock()
	defer iter.lock.RUnlock()
	if iter.elem != nil {
		iter.elem = iter.elem.levels[0]
	}
}
func (iter *SkipListIter) Valid() bool {
	return iter.elem != nil
}
func (iter *SkipListIter) Rewind() {
	iter.elem = iter.header.levels[0]
}
func (iter *SkipListIter) Item() Item {
	return iter.elem
}
func (iter *SkipListIter) Close() error {
	return nil
}

func (iter *SkipListIter) Seek(key []byte) {
}
