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

package lsm

import (
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils/codec"
)

type Iterator struct {
	it    iterator.Item
	iters []iterator.Iterator
}
type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}

// 创建迭代器
func (lsm *LSM) NewIterator(opt *iterator.Options) iterator.Iterator {
	iter := &Iterator{}
	iter.iters = make([]iterator.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, imm := range lsm.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.NewIterator(opt))
	return iter
}
func (iter *Iterator) Next() {
	iter.iters[0].Next()
}
func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}
func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}
func (iter *Iterator) Item() iterator.Item {
	return iter.iters[0].Item()
}
func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}

// 内存表迭代器
type memIterator struct {
	innerIter iterator.Iterator
}

func (m *memTable) NewIterator(opt *iterator.Options) iterator.Iterator {
	return &memIterator{innerIter: m.sl.NewSkipListIterator()}
}
func (iter *memIterator) Next() {
	iter.innerIter.Next()
}
func (iter *memIterator) Valid() bool {
	return iter.innerIter.Valid()
}
func (iter *memIterator) Rewind() {
	iter.innerIter.Rewind()
}
func (iter *memIterator) Item() iterator.Item {
	return iter.innerIter.Item()
}
func (iter *memIterator) Close() error {
	return iter.innerIter.Close()
}
func (iter *memIterator) Seek(key []byte) {
}

// levelManager上的迭代器
type levelIterator struct {
	it    *iterator.Item
	iters []*Iterator
}

func (lm *levelManager) NewIterator(options *iterator.Options) iterator.Iterator {
	return &levelIterator{}
}
func (iter *levelIterator) Next() {
}
func (iter *levelIterator) Valid() bool {
	return false
}
func (iter *levelIterator) Rewind() {

}
func (iter *levelIterator) Item() iterator.Item {
	return &Item{}
}
func (iter *levelIterator) Close() error {
	return nil
}

func (iter *levelIterator) Seek(key []byte) {
}
