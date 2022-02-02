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

package vlog

import (
	"github.com/hardcore-os/corekv/utils"
)

type Options struct {
}

// VLog
type VLog struct {
	closer *utils.Closer
}

// Close 关闭资源
func (v *VLog) Close() error {
	return nil
}

// NewVLog
func NewVLog(opt *Options) *VLog {
	v := &VLog{}
	v.closer = utils.NewCloser()
	return v
}

// StartGC
func (v *VLog) StartGC() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.CloseSignal:
			return
		}
		// gc logic...
	}
}

// Set
func (v *VLog) Set(entry *utils.Entry) error {
	return nil
}

func (v *VLog) Get(entry *utils.Entry) (*utils.Entry, error) {
	// valuePtr := utils.ValuePtrDecode(entry.Value)
	return nil, nil
}
