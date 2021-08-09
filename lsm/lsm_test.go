package lsm

import (
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/stretchr/testify/assert"
	"testing"
)

// 对level 管理器的功能测试
func TestLevels(t *testing.T) {
	// 初始化opt
	opt := &Options{}
	levelLive := func() {
		// 初始化
		levels := newLevelManager(opt)
		defer func() {}()
		// 测试 flush
		imm := &memTable{
			wal: file.OpenWalFile(&file.Options{}),
			sl:  utils.NewSkipList(),
		}
		assert.Nil(t,levels.flush(imm))
		// 从levels中进行GET
		v, err := levels.Get([]byte("Hello"))
		assert.Nil(t, err)
		assert.Equal(t,codec.Entry{Value:[]byte("Corekv"),}.Value,v)
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.Value)
		// 关闭levels
		assert.Nil(t,levels.close())
	}
	// 运行N次测试多个sst的影响
	for i := 0; i < 10; i++ {
		levelLive()
	}
}

// 对level管理器的性能测试
