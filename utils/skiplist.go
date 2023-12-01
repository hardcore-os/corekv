package utils

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header   *Element
	rand     *rand.Rand
	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!
	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}
	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel - 1,
		rand:     r,
	}
	// return nil
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//implement me here!!!
	prevs := make([]*Element, list.maxLevel+1)

	key := data.Key
	// keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	// for i := maxLevel; i >= 0; i-- {
	// 	for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
	// 		if comp := list.compare(keyScore, key, ne); comp <= 0 {
	// 			if comp == 0 {
	// 				// 更新数据
	// 				ne.entry = data
	// 				return nil
	// 			} else {
	// 				prev = ne
	// 			}
	// 		} else {
	// 			break
	// 		}
	// 	}
	// 	prevs[i] = prev
	// }

	for i := maxLevel; i >= 0; i-- {
		for nxt := prev.levels[i]; nxt != nil; nxt = prev.levels[i] {
			if bytes.Compare(key, nxt.entry.Key) <= 0 {
				if bytes.Compare(key, nxt.entry.Key) == 0 {
					// fmt.Println(nxt.entry.Key)
					// fmt.Println(key)
					nxt.entry = data
					return nil
				}
				break
			}
			prev = nxt
		}
		prevs[i] = prev
	}

	randLevel, keyScore := list.randLevel(), list.calcScore(key)
	e := newElement(keyScore, data, randLevel)

	for i := randLevel - 1; i >= 0; i-- {
		// fmt.Println(i, "dada")
		// fmt.Println(len(e.levels))
		ne := prevs[i].levels[i]
		prevs[i].levels[i] = e
		e.levels[i] = ne
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	// keyScore := list.calcScore(key)
	header, maxLevel := list.header, list.maxLevel
	prev := header
	// for i := maxLevel; i >= 0; i-- {
	// 	for ne := prev.levels[i]; ne != nil; ne = prev.levels[i] {
	// 		if comp := list.compare(keyScore, key, ne); comp <= 0 {
	// 			if comp == 0 {
	// 				return ne.entry
	// 			} else {
	// 				prev = ne
	// 			}
	// 		} else {
	// 			break
	// 		}
	// 	}
	// }

	for i := maxLevel; i >= 0; i-- {
		for nxt := prev.levels[i]; nxt != nil; nxt = prev.levels[i] {
			if bytes.Compare(key, nxt.entry.Key) <= 0 {
				if bytes.Compare(key, nxt.entry.Key) == 0 {
					// fmt.Println(nxt.entry.Key)
					// fmt.Println(key)
					return nxt.entry
				}
				break
			}
			prev = nxt
		}
	}
	return nil
}

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
	//implement me here!!!
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
	//implement me here!!!
	// var level int
	i := 1
	for ; ; i++ {
		if rand.Intn(2) == 0 {
			return i
		}
	}
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
