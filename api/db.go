package api

import (
	"time"
)

type (
	// kv引擎对外提供的功能集合
	Api interface {
		Open(opt *Options) (*DB, error)
		Set(data *KV) error
		Get(key string) (*KV, error)
		Del(key string) error
		NewIterator(opt int) Iterator
		Info()
		SetTTL(key string, value []byte, ts time.Duration)
	}

	// 迭代器
	Iterator interface{}

	// 参数化配置对象
	Options interface{}

	// KV 对象的封装
	KV interface{}
)

// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
type DB struct {
}
