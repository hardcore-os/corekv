package lsm

import "github.com/logikoisto/coreKV/file"

type table struct {
	ss *file.SSTable
}

func openTable(opt *Options) *table {
	return &table{ss: file.OpenSStable(&file.Options{})}
}
