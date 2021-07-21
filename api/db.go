package api

type (
	// coreKV对外提供的功能集合
	CoreAPI interface {
		Set(data *Entry) error
		Get(key []byte) (*Entry, error)
		Del(key []byte) error
		NewIterator(opt *IteratorOptions) Iterator
		Stats() *Info
		Close() error
	}

	// DB 对外暴露的接口对象 全局唯一，持有各种资源句柄
	DB struct {
		opt *Options
	}
)

func Open(options *Options) *DB {
	return &DB{opt: options}
}

func (db *DB) Close() error {
	return nil
}
func (db *DB) Del(key []byte) error {
	return nil
}
func (db *DB) Set(data *Entry) error {
	return nil
}
func (db *DB) Get(key []byte) (*Entry, error) {
	return &Entry{}, nil
}
func (db *DB) Stats() *Info {
	return &Info{}
}
