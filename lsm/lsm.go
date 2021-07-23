package lsm

type LSM struct {
	memTable  *memTable
	immTables []*memTable
	levels    *levelManager
	option    *Options
}

//Options
type Options struct {
}

// NewLSM
func NewLSM(opt *Options) *LSM {
	lsm := &LSM{option: opt}
	// 启动DB恢复过程加载wal，如果没有恢复内容则创建新的内存表
	lsm.memTable, lsm.immTables = recovery(opt)
	// 初始化levelManager
	lsm.levels = newLevelManager(opt)
	return &LSM{}
}

func (lsm *LSM) StartMerge() {}
