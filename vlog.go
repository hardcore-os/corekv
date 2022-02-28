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

package corekv

import (
	"sync"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
)

// valueLog
type valueLog struct {
}

func (vlog *valueLog) newValuePtr(e *utils.Entry) (*utils.ValuePtr, error) {
	return nil, nil
}
func (vlog *valueLog) open(db *DB, ptr *utils.ValuePtr, replayFn utils.LogEntry) error {
	return nil
}

// Read reads the value log at a given location.
// TODO: Make this read private.
func (vlog *valueLog) read(vp *utils.ValuePtr) ([]byte, func(), error) {
	return nil, nil, nil
}

// write 并不是并发安全的
func (vlog *valueLog) write(reqs []*request) error {
	return nil
}

func (vlog *valueLog) close() error {
	return nil
}

func (vlog *valueLog) runGC(discardRatio float64, head *utils.ValuePtr) error {
	return nil
}

func (vlog *valueLog) doRunGC(lf *file.LogFile, discardRatio float64) (err error) {
	return nil
}

//重写
func (vlog *valueLog) rewrite(f *file.LogFile) error {
	return nil
}

func (vlog *valueLog) deleteLogFile(lf *file.LogFile) error {
	return nil
}

// validateWrites  可以检查当前的req是否能写入vlog日志，一个vlog日志最大4GB
func (vlog *valueLog) validateWrites(reqs []*request) error {
	return nil
}

// estimateRequestSize returns the size that needed to be written for the given request.
func estimateRequestSize(req *request) uint64 {
	size := uint64(0)
	return size
}

// getUnlockCallback will returns a function which unlock the logfile if the logfile is mmaped.
// otherwise, it unlock the logfile and return nil.
func (vlog *valueLog) getUnlockCallback(lf *file.LogFile) func() {
	return lf.Lock.RUnlock
}

// readValueBytes return vlog entry slice and read locked log file. Caller should take care of
// logFile unlocking.
func (vlog *valueLog) readValueBytes(vp *utils.ValuePtr) ([]byte, *file.LogFile, error) {
	return nil, nil, nil
}

// Gets the logFile and acquires and RLock() for the mmap. You must call RUnlock on the file
// (if non-nil)
func (vlog *valueLog) getFileRLocked(vp *utils.ValuePtr) (*file.LogFile, error) {

	return nil, nil
}

func (vlog *valueLog) woffset() uint32 {
	return 0
}

func (vlog *valueLog) populateFilesMap() error {

	return nil
}

func (vlog *valueLog) createVlogFile(fid uint32) (*file.LogFile, error) {
	return nil, nil
}

// sortedFids returns the file id's not pending deletion, sorted.  Assumes we have shared access to
// filesMap.
func (vlog *valueLog) sortedFids() []uint32 {
	return nil
}

func (vlog *valueLog) replayLog(lf *file.LogFile, offset uint32, replayFn utils.LogEntry) error {
	return nil
}
func (db *DB) replayFunction() func(*utils.Entry, *utils.ValuePtr) error {
	return nil
}

// updateHead should not be called without the db.Lock() since db.vhead is used
// by the writer go routines and memtable flushing goroutine.
func (db *DB) updateHead(ptrs []*utils.ValuePtr) {
}

// sync  同步一下，刷盘
func (vlog *valueLog) sync(fid uint32) error {
	return nil
}

// StartGC
func (v *valueLog) startGC() {

}

// Set
func (v *valueLog) set(entry *utils.Entry) error {
	return nil
}

func (v *valueLog) get(entry *utils.Entry) (*utils.Entry, error) {
	// valuePtr := utils.ValuePtrDecode(entry.Value)
	return nil, nil
}

// lfDiscardStats 记录丢弃key的数据
// lfDiscardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type lfDiscardStats struct {
}

func (vlog *valueLog) flushDiscardStats() {
}

// 请求池
var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

// request
type request struct {
}

func (req *request) reset() {

}

// GC 部分
// 选择需要gc的log文件
func (vlog *valueLog) pickLog(head *utils.ValuePtr) (files []*file.LogFile) {
	return nil
}
func (vlog *valueLog) waitOnGC(lc *utils.Closer) {
}

type reason struct {
}
