package file

import (
	"bufio"
	"encoding/csv"
	"io"

	"github.com/hardcore-os/corekv/utils"
)

type Manifest struct {
	f      CoreFile
	tables [][]string // l0-l7 的sst file name
}

// WalFile
func (mf *Manifest) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// Tables 获取table的list
func (mf *Manifest) Tables() [][]string {
	return mf.tables
}

// OpenManifest
func OpenManifest(opt *Options) *Manifest {
	mf := &Manifest{
		f:      OpenMockFile(opt),
		tables: make([][]string, utils.MaxLevelNum),
	}
	reader := csv.NewReader(bufio.NewReader(mf.f))
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		// TODO: csv 读取manifest
		for i := 0; i < utils.MaxLevelNum; i++ {
			for j, tableName := range line {
				mf.tables[i][j] = tableName
			}
		}
	}
	return mf
}
