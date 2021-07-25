package iterator

// 迭代器
type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() *Item
	Close() error
}
type Item struct {
	key []byte
	val []byte
}
type Options struct {
	Prefix []byte
	IsAsc  bool
}

func NewIterator(opt *Options) Iterator {
	return &DBIterator{}
}

type IteratorOptions struct {
}

func (it *Item) Key() []byte {
	return it.key
}
func (it *Item) Value() []byte {
	return it.val
}

type DBIterator struct {
}

func (iter *DBIterator) Next() {

}
func (iter *DBIterator) Valid() bool {
	return false
}
func (iter *DBIterator) Rewind() {

}
func (iter *DBIterator) Item() *Item {
	return &Item{}
}
func (iter *DBIterator) Close() error {
	return nil
}
