// Copyright 2021 logicrec Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package corekv

import (
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils/codec"
)

type DBIterator struct {
	iters []iterator.Iterator
}
type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}
func (db *DB) NewIterator(opt *iterator.Options) iterator.Iterator {
	dbIter := &DBIterator{}
	dbIter.iters = make([]iterator.Iterator, 0)
	dbIter.iters = append(dbIter.iters, db.lsm.NewIterator(opt))
	return dbIter
}

func (iter *DBIterator) Next() {
	iter.iters[0].Next()
}
func (iter *DBIterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *DBIterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *DBIterator) Item() iterator.Item {
	return iter.iters[0].Item()
}
func (iter *DBIterator) Close() error {
	return nil
}
func (iter *DBIterator) Seek(key []byte) {
}
