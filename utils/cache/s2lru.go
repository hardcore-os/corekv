package cache

import "container/list"

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota
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
	newitem.stage = 1

	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	delete(slru.data, item.key)

	*item = newitem

	slru.data[item.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[bitem.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}
