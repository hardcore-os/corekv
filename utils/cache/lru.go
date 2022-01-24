package cache

import "container/list"

type windowLRU struct {
	data map[uint64]*list.Element
	cap  int
	list *list.List
}

type storeItem struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

func (lru *windowLRU) add(newitem storeItem) (eitem storeItem, evicted bool) {
	if lru.list.Len() < lru.cap {
		lru.data[newitem.key] = lru.list.PushFront(&newitem)
		return storeItem{}, false
	}

	evictItem := lru.list.Back()
	item := evictItem.Value.(*storeItem)

	delete(lru.data, item.key)

	eitem, *item = *item, newitem

	lru.data[item.key] = evictItem
	lru.list.MoveToFront(evictItem)
	return eitem, true
}

func (lru *windowLRU) get(v *list.Element) {
	lru.list.MoveToFront(v)
}
