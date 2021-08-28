package lsm

import (
	"testing"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/stretchr/testify/assert"
)

// 对level 管理器的功能测试
func TestLevels(t *testing.T) {
	entrys := []*codec.Entry{
		{Key: []byte("hello0"), Value: []byte("world0"), ExpiresAt: uint64(0)},
		{Key: []byte("hello1"), Value: []byte("world1"), ExpiresAt: uint64(0)},
		{Key: []byte("hello2"), Value: []byte("world2"), ExpiresAt: uint64(0)},
		{Key: []byte("hello3"), Value: []byte("world3"), ExpiresAt: uint64(0)},
		{Key: []byte("hello4"), Value: []byte("world4"), ExpiresAt: uint64(0)},
		{Key: []byte("hello5"), Value: []byte("world5"), ExpiresAt: uint64(0)},
		{Key: []byte("hello6"), Value: []byte("world6"), ExpiresAt: uint64(0)},
		{Key: []byte("hello7"), Value: []byte("world7"), ExpiresAt: uint64(0)},
	}
	// 初始化opt
	opt := &Options{
		WorkDir: "../work_test",
	}
	levelLive := func() {
		// 初始化
		levels := newLevelManager(opt)
		defer func() { _ = levels.close() }()
		// 构建内存表
		imm := &memTable{
			wal: file.OpenWalFile(&file.Options{Name: "00001.mem", Dir: opt.WorkDir}),
			sl:  utils.NewSkipList(),
		}
		for _, entry := range entrys {
			imm.set(entry)
		}
		// 测试 flush
		assert.Nil(t, levels.flush(imm))
		// 从levels中进行GET
		v, err := levels.Get([]byte("hello7"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("world7"), v.Value)
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)
		// 关闭levels
		assert.Nil(t, levels.close())
	}
	// 运行N次测试多个sst的影响
	for i := 0; i < 10; i++ {
		levelLive()
	}
}

// 对level管理器的性能测试
