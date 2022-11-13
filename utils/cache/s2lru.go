package cache

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	newitem.stage = STAGE_ONE

	if slru.stageOne.Len() < slru.stageOneCap || slru.stageOne.Len()+slru.stageTwo.Len() < slru.Len() {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	backEle := slru.stageOne.Back()
	backItem := backEle.Value.(*storeItem)

	delete(slru.data, backItem.key)

	*backItem = newitem

	slru.data[newitem.key] = backEle
	slru.stageOne.MoveToFront(backEle)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		return
	}

	twoBack := slru.stageTwo.Back()
	twoBackItem := twoBack.Value.(*storeItem)

	*twoBackItem, *item = *item, *twoBackItem

	slru.data[twoBackItem.key] = v
	slru.data[item.key] = twoBack

	twoBackItem.stage = STAGE_ONE
	item.stage = STAGE_TWO

	slru.stageTwo.MoveToFront(twoBack)
	slru.stageOne.MoveToFront(v)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	//如果 slru 的容量未满，不需要淘汰
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	// 如果已经满了，则需要从20%的区域淘汰数据，这里直接从尾部拿最后一个元素即可
	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
