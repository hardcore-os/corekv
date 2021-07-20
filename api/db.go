package api

type (
	// coreKV对外提供的功能集合
	CoreAPI interface {
		Set(data *Entry) error
		Get(key []byte) (*Entry, error)
		Del(key []byte) error
		NewIterator(opt *IteratorOptions) Iterator
		Info() *Infos
		CLose() error
	}
)

// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
type DB struct {
	opt *Options
}

func Open(options *Options) *DB {
	return &DB{opt: options}
}

func (db *DB) Close() error {
	return nil
}
