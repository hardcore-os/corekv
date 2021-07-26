package corekv

import (
	"github.com/hardcore-os/corekv/codec"
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/lsm"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/vlog"
)

type (
	// coreKV对外提供的功能集合
	CoreAPI interface {
		Set(data *codec.Entry) error
		Get(key []byte) (*codec.Entry, error)
		Del(key []byte) error
		NewIterator(opt *iterator.Options) iterator.Iterator
		Info() *Stats
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
	DB struct {
		opt   *Options
		lsm   *lsm.LSM
		vlog  *vlog.VLog
		stats *Stats
	}
)

func Open(options *Options) *DB {
	db := &DB{opt: options}
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{})
	// 初始化vlog结构
	db.vlog = vlog.NewVLog(&vlog.Options{})
	// 初始化统计信息
	db.stats = newStats(options)
	// 启动 sstable 的合并压缩过程
	go db.lsm.StartMerge()
	// 启动 vlog gc 过程
	go db.vlog.StartGC()
	// 启动 info 统计过程
	go db.stats.StartStats()
	return db
}

func (db *DB) Close() error {
	if err := db.lsm.Close(); err != nil {
		return err
	}
	if err := db.vlog.Close(); err != nil {
		return err
	}
	if err := db.stats.close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Del(key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	return nil
}
func (db *DB) Set(data *codec.Entry) error {
	// 做一些必要性的检查
	// 如果value 大于一个阈值 则创建值指针，并将其写入vlog中
	if utils.ValueSize(data.Value) > db.opt.ValueThreshold {
		valuePtr := vlog.NewValuePtr(data)
		// 先写入vlog不会有事务问题，因为如果lsm写入失败，vlog会在GC阶段清理无效的key
		if err := db.vlog.Set(valuePtr); err != nil {
			return err
		}
	}
	// 写入LSM
	if err := db.lsm.Set(data); err != nil {
		return err
	}
	return nil
}
func (db *DB) Get(key []byte) (*codec.Entry, error) {
	var entry *codec.Entry
	// 检查输入
	// 从内存表中读取数据
	if entry = db.lsm.Get(key); entry != nil {
		return entry, nil
	}
	// 没读到从table中读取数据，迭代不同的level去读取
	return &codec.Entry{}, nil
}
func (db *DB) Info() *Stats {
	// 读取stats结构，打包数据并返回
	return &Stats{}
}
