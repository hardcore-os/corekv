package lsm

import (
	"testing"
)

// 对level 管理器的测试
func TestLevels(t *testing.T) {
	// 初始化
	levels := newLevelManager(&Options{})
	// 关闭levels
	defer func() { _ = levels.close() }()
	// 从levels中进行GET
	if entry, err := levels.Get([]byte("Hello")); err == nil {
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.Value)
	} else {
		t.Fatal(err)
	}
	// 测试 在level上的iter
	// 测试 cache
	// 测试 flush
	// 测试 merge
	// 测试 load manifest 是否正确
}
