package file

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
)

// SSTable 文件的内存封装
type SSTable struct {
	f      *MockFile
	maxKey []byte
	minKey []byte
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
			tmp := strings.Split(dataStr, ",")
			ss.maxKey = []byte(tmp[len(tmp)-1])
			ss.minKey = []byte(tmp[0])
		}
	}
	return ss.indexs
}

// MaxKey 当前最大的key
func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

// MinKey 当前最小的key
func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

// FID 获取fid
func (ss *SSTable) FID() string {
	return ss.fid
}

// SaveSkipListToSSTable 将跳表序列化到sst文件中
// TODO 设计sst文件格式，并重写此处flush逻辑
func (ss *SSTable) SaveSkipListToSSTable(sl *utils.SkipList) error {
	iter := sl.NewIterator(&iterator.Options{})
	indexs, datas, idx := make([]string, 0), make([]string, 0), 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item().Entry()
		indexs = append(indexs, string(item.Key))
		indexs = append(indexs, fmt.Sprintf("%d", idx))
		datas = append(datas, string(item.Value))
		idx++
	}
	ssData := make(map[string]string, 0)
	ssData["idx"] = strings.Join(indexs, ",")
	ssData["data"] = strings.Join(datas, ",")
	bData, err := json.Marshal(ssData)
	if err != nil {
		return err
	}
	if _, err := ss.f.Write(bData); err != nil {
		return err
	}
	ss.indexs = []byte(ssData["idx"])
	return nil
}

// LoadData 加载数据块
func (ss *SSTable) LoadData() (blocks [][]byte, offsets []int) {
	ss.f.f.Seek(0, io.SeekStart)
	bv, err := ioutil.ReadAll(ss.f)
	utils.Panic(err)
	m := make(map[string]interface{}, 0)
	json.Unmarshal(bv, &m)
	if data, ok := m["data"]; !ok {
		panic("sst data is nil")
	} else {
		// TODO 所有的数据都放在一个 block中
		dd := data.(string)
		blocks = append(blocks, []byte(dd))
		offsets = append(offsets, 0)
	}
	return blocks, offsets
}
