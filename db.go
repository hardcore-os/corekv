package coreKV

import (
	"github.com/logikoisto/coreKV/file"
	"github.com/logikoisto/coreKV/interator"
	"github.com/logikoisto/coreKV/lsm"
)

type (
	// coreKV对外提供的功能集合
	CoreAPI interface {
		Set(data *Entry) error
		Get(key []byte) (*Entry, error)
		Del(key []byte) error
		NewIterator(opt *interator.IteratorOptions) interator.Iterator
		Info() *Stats
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
	DB struct {
		opt   *Options
		lsm   *lsm.LSM
		vlog  *file.VLog
		stats *Stats
	}
)

func Open(options *Options) *DB {
	db := &DB{opt: options}
	// 初始化LSM结构
	db.lsm = lsm.NewLSM(&lsm.Options{})
	// 初始化vlog结构
	db.vlog = file.NewVLog(&file.Options{})
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
	// 逐步完成所有内存表的flush
	// 在保证所有内存表flush成功后，清理wal文件
	return nil
}
func (db *DB) Del(key []byte) error {
	// 写入一个值为nil的entry 作为墓碑消息实现删除
	return nil
}
func (db *DB) Set(data *Entry) error {
	// 写入LSM
	// 如果value 大于一个阈值 则创建值指针，并将其写入vlog中
	return nil
}
func (db *DB) Get(key []byte) (*Entry, error) {
	// 从内存表中读取数据
	// 没读到从table中读取数据，迭代不同的level去读取
	return &Entry{}, nil
}
func (db *DB) Info() *Stats {
	// 读取stats结构，打包数据并返回
	return &Stats{}
}
