package lsm

import "github.com/hardcore-os/corekv/file"

type table struct {
	ss      *file.SSTable
	idxData []byte
}

func openTable(opt *Options, tableName string) *table {
	t := &table{ss: file.OpenSStable(&file.Options{Name: tableName})}
	// 加载ss文件 索引
	return t
}
