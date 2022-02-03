// Copyright 2021 logicrec Project Authors
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

import "github.com/hardcore-os/corekv/utils"

type Stats struct {
	closer   *utils.Closer
	EntryNum int64 // 存储多少个kv数据
}

// Close
func (s *Stats) close() error {
	return nil
}

// StartStats
func (s *Stats) StartStats() {
	defer s.closer.Done()
	for {
		select {
		case <-s.closer.CloseSignal:
			return
		}
		// stats logic...
	}
}

// NewStats
func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser()
	s.EntryNum = 1 // 这里直接写
	return s
}
