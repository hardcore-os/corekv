package api

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
func (db *DB) NewIterator(opt *IteratorOptions) Iterator {
	return &DBIterator{}
}
