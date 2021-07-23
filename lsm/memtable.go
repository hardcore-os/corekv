package lsm

import (
	"github.com/logikoisto/coreKV/file"
	"github.com/logikoisto/coreKV/utils"
)

// MemTable
type memTable struct {
	wal *file.WalFile
	sl  *utils.Skiplist
}

//recovery
func recovery(opt *Options) (*memTable, []*memTable) {
	fileOpt := &file.Options{}
	return &memTable{wal: file.OpenWalFile(fileOpt), sl: utils.NewSkipList()}, []*memTable{}
}
