package file

import (
	"bufio"
	"encoding/csv"
	"github.com/hardcore-os/corekv/utils"
	"io"
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
		_ = line
	}
	return mf
}
