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
	"os"
	"sync"

	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

type WalFile struct {
	lock *sync.RWMutex
	f    *MmapFile
}

// WalFile
func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return nil
}
func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &WalFile{f: omf, lock: &sync.RWMutex{}}
}

func (wf *WalFile) Write(entry *codec.Entry) error {
	// 落预写日志简单的同步写即可
	// 序列化为磁盘结构
	walData := codec.WalCodec(entry)
	wf.lock.Lock()
	fileData, _, err := wf.f.AllocateSlice(len(walData), 0) // TODO 这里要维护offset才行
	utils.Panic(err)
	copy(fileData, walData)
	wf.lock.Unlock()
	return nil
}
