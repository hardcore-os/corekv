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
	level := 0
	for {
		if level > utils.MaxLevelNum {
			break
		}
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if len(mf.tables[level]) == 0 {
			mf.tables[level] = make([]string, len(line))
		}
		for j, tableName := range line {
			mf.tables[level][j] = tableName
		}
		level++
	}
	return mf
}
