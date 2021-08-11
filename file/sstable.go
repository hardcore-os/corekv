package file

import (
	"encoding/json"
	"io/ioutil"

	"github.com/hardcore-os/corekv/utils"
)

// SSTable 文件的内存封装
type SSTable struct {
	f      *MockFile
	indexs []byte
	fid    string
}

// OpenSStable 打开一个 sst文件
func OpenSStable(opt *Options) *SSTable {
	return &SSTable{f: OpenMockFile(opt), fid: utils.FID(opt.Name)}
}

// Indexs 获取sst文件索引
func (ss *SSTable) Indexs() []byte {
	if len(ss.indexs) == 0 {
		bv, _ := ioutil.ReadAll(ss.f)
		m := make(map[string]interface{}, 0)
		json.Unmarshal(bv, &m)
		if idx, ok := m["idx"]; !ok {
			panic("sst idx is nil")
		} else {
			dataStr, _ := idx.(string) // hello,0
			ss.indexs = []byte(dataStr)
		}
	}
	return ss.indexs
}

// FID 获取fid
func (ss *SSTable) FID() string {
	return ss.fid
}
