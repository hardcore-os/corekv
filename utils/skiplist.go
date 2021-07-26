package utils

import "github.com/hardcore-os/corekv/codec"

// 跳表的基础实现
type Skiplist struct {
	maxLevel int
	head     *node
}

func (sl *Skiplist) Close() error {
	return nil
}

// Add
func (sl *Skiplist) Add(entry *codec.Entry) error {
	// 简单的存储在单连表中
	sl.head.next = &node{
		member: entry,
	}
	return nil
}

//Search
func (sl *Skiplist) Search(key []byte) *codec.Entry {
	return sl.head.next.member
}

type node struct {
	member *codec.Entry // 存储的int 也作为排序的score
	next   *node
	pre    *node
	levels []*node
}

func NewSkipList() *Skiplist {
	return &Skiplist{
		maxLevel: 0,
		head: &node{
			member: nil,
			next:   nil,
			levels: make([]*node, 0, 64),
		},
	}
}
