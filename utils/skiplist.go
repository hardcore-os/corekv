package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/utils/codec"
	"math/rand"
	"time"
)

const (
	DefaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
}

func NewSkipList() *SkipList {
	source := rand.NewSource(time.Now().UnixNano())

	return &SkipList{
		header: &Element{
			levels: make([]*Element, DefaultMaxLevel),
			Key:    nil,
			Val:    nil,
			score:  0,
		},
		rand:     rand.New(source),
		maxLevel: DefaultMaxLevel,
		length:   0,
	}
}

type Element struct {
	levels []*Element
	Key    []byte
	Val    []byte
	score  float64
}

func newElement(score float64, key, val []byte, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		Key:    key,
		Val:    val,
		score:  score,
	}
}

func (list *SkipList) Add(data *codec.Entry) error {
	score := list.calcScore(data.Key)
	var elem *Element

	max := len(list.header.levels)
	prevElem := list.header

	var prevElemHeaders [DefaultMaxLevel]*Element

	for i := max - 1; i >= 0; {
		//keep visit path here
		prevElemHeaders[i] = prevElem

		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, data.Key, next); comp <= 0 {
				if comp == 0 {
					elem = next
					elem.Val = data.Value
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

	elem = newElement(score, data.Key, data.Value, level)

	//to add elem to the skiplist

	for i := 0; i < level; i++ {
		elem.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = elem
	}

	list.length++
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	if list.length == 0 {
		return nil
	}

	score := list.calcScore(key)

	prevElem := list.header
	i := len(list.header.levels) - 1

	for i >= 0 {
		for next := prevElem.levels[i]; next != nil; next = prevElem.levels[i] {
			if comp := list.compare(score, key, next); comp <= 0 {
				if comp == 0 {
					return codec.NewEntry(next.Key, next.Val)
				}
				break
			}

			prevElem = next
		}

		topLevel := prevElem.levels[i]

		for i--; i >= 0 && prevElem.levels[i] == topLevel; i-- {

		}
	}
	return
}

func (list *SkipList) Remove(key []byte) error {
	score := list.calcScore(key)

	max := len(list.header.levels)
	prevElem := list.header

	var prevElemHeaders [DefaultMaxLevel]*Element
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
	if score == next.score {
		return bytes.Compare(key, next.Key)
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
	for ; i < list.maxLevel; i++ {
		if list.rand.Intn(1000)%2 == 0 {
			return i
		}
	}
	return i
}
