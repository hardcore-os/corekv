package iterator

import "github.com/hardcore-os/corekv/utils/codec"

// 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
}
type Item interface {
	Entry() *codec.Entry
}
type Options struct {
	Prefix []byte
	IsAsc  bool
}
