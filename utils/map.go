package utils

import "sync"

type CoreMap struct {
	m sync.Map
}

// NewMap
func NewMap() *CoreMap {
	return &CoreMap{m: sync.Map{}}
}

// Get
func (c *CoreMap) Get(key interface{}) (interface{}, bool) {
	return c.m.Load(key)
}

// Set
func (c *CoreMap) Set(key, value interface{}) {
	c.m.Store(key, value)
}

// Range
func (c *CoreMap) Range(f func(key, value interface{}) bool) {
	c.m.Range(f)
}
