package cache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCacheBasicCRUD(t *testing.T) {
	cache := NewCache(5)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		if ok {
			assert.Equal(t, val, res)
			continue
		}
		assert.Equal(t, res, nil)

	}
}
