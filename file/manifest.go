// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package file

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec/pb"
)

// ManifestFile 维护sst文件元信息的文件
// manifest 比较特殊，不能使用mmap，需要保证实时的写入
type ManifestFile struct {
	opt      *Options
	f        *os.File
	lock     sync.Mutex
	manifest *Manifest
}

// Manifest corekv 元数据状态维护
type Manifest struct {
	Levels []levelManifest
	Tables map[uint64]TableManifest
}

// TableManifest 包含sst的基本信息
type TableManifest struct {
	Level    uint8
	Checksum []byte
}
type levelManifest struct {
	Tables map[uint64]struct{} // Set of table id's
}

// OpenManifestFile 打开manifest文件
func OpenManifestFile(opt *Options) (*ManifestFile, error) {
	path := filepath.Join(opt.Dir, utils.ManifestFilename)
	mf := &ManifestFile{lock: sync.Mutex{}, opt: opt}
	f, err := os.OpenFile(path, utils.DefaultFileFlag, utils.DefaultFileMode)
	// 如果打开失败 则尝试创建一个新的 manifest file
	if err != nil {
		if !os.IsNotExist(err) {
			return mf, err
		}
		m := createManifest()
		fp, netCreations, err := helpRewrite(opt.Dir, m)
		utils.EqualPanic(netCreations == 0, utils.ErrReWriteFailure)
		if err != nil {
			return mf, err
		}
		mf.f = fp
		mf.manifest = m
		return mf, nil
	}

	// 如果打开 则对manifest文件重放
	manifest, truncOffset, err := ReplayManifestFile(fp)
	if err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}
	if !readOnly {
		// Truncate file so we don't have a half-written entry at the end.
		if err := fp.Truncate(truncOffset); err != nil {
			_ = fp.Close()
			return nil, Manifest{}, err
		}
	}
	if _, err = fp.Seek(0, io.SeekEnd); err != nil {
		_ = fp.Close()
		return nil, Manifest{}, err
	}

	mf := &manifestFile{
		fp:                        fp,
		directory:                 dir,
		manifest:                  manifest.clone(),
		deletionsRewriteThreshold: deletionsThreshold,
	}
	return mf
}

// ReplayManifestFile 对已经存在的manifest文件重新应用所有状态变更
func ReplayManifestFile(fp *os.File) (ret Manifest, truncOffset int64, err error) {
	r := bufio.NewReader(fp)
	var magicBuf [8]byte
	if _, err := io.ReadFull(&r, magicBuf[:]); err != nil {
		return Manifest{}, 0, errBadMagic
	}
	if !bytes.Equal(magicBuf[0:4], magicText[:]) {
		return Manifest{}, 0, errBadMagic
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != magicVersion {
		return Manifest{}, 0,
			fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, magicVersion)
	}

	build := createManifest()
	var offset int64
	for {
		offset = r.count
		var lenCrcBuf [8]byte
		_, err := io.ReadFull(&r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return Manifest{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		var buf = make([]byte, length)
		if _, err := io.ReadFull(&r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return Manifest{}, 0, err
		}
		if crc32.Checksum(buf, y.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
			return Manifest{}, 0, errBadChecksum
		}

		var changeSet pb.ManifestChangeSet
		if err := changeSet.Unmarshal(buf); err != nil {
			return Manifest{}, 0, err
		}

		if err := applyChangeSet(&build, &changeSet); err != nil {
			return Manifest{}, 0, err
		}
	}

	return build, offset, err
}

func createManifest() *Manifest {
	levels := make([]levelManifest, 0)
	return &Manifest{
		Levels: levels,
		Tables: make(map[uint64]TableManifest),
	}
}

// asChanges returns a sequence of changes that could be used to recreate the Manifest in its
// present state.
func (m *Manifest) asChanges() []*pb.ManifestChange {
	changes := make([]*pb.ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, int(tm.Level), tm.Checksum))
	}
	return changes
}
func newCreateChange(id uint64, level int, checksum []byte) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:       id,
		Op:       pb.ManifestChange_CREATE,
		Level:    uint32(level),
		Checksum: checksum,
	}
}

func helpRewrite(dir string, m *Manifest) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, utils.ManifestRewriteFilename)
	// We explicitly sync.
	fp, err := os.OpenFile(rewritePath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)
	copy(buf[0:4], utils.MagicText[:])
	binary.BigEndian.PutUint32(buf[4:8], uint32(utils.MagicVersion))

	netCreations := len(m.Tables)
	changes := m.asChanges()
	set := pb.ManifestChangeSet{Changes: changes}

	changeBuf, err := set.Marshal()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(changeBuf)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(changeBuf, utils.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestPath := filepath.Join(dir, utils.ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, utils.DefaultFileFlag, utils.DefaultFileMode)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := utils.SyncDir(dir); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil
}

// Close 关闭文件
func (mf *ManifestFile) Close() error {
	if err := mf.f.Close(); err != nil {
		return err
	}
	return nil
}

// AppendSST 存储level表到manifest的level中
func (mf *ManifestFile) AppendSST(levelNum int, cell *Cell) (err error) {
	mf.tables[levelNum] = append(mf.tables[levelNum], cell)
	res := make([][]string, len(mf.tables))
	for i, cells := range mf.tables {
		res[i] = make([]string, 0)
		for _, cell := range cells {
			res[i] = append(res[i], cell.SSTName)
		}
	}
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	// fmt.Println(string(data))
	// panic(data)
	// err = mf.f.Delete()
	// if err != nil {
	// 	return err
	// }
	mf.lock.Lock()
	defer mf.lock.Unlock()
	// TODO 保留旧的MANIFEST文件作为检查点，当前直接截断
	fileData, _, err := mf.f.AllocateSlice(len(data), 0)
	utils.Panic(err)
	copy(fileData, data)
	return err
}
