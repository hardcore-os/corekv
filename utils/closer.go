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

package utils

import "sync"

// Closer _用于资源回收的信号控制
type Closer struct {
	waiting     sync.WaitGroup
	closeSignal chan struct{}
}

// NewCloser _
func NewCloser(i int) *Closer {
	closer := &Closer{waiting: sync.WaitGroup{}}
	closer.waiting.Add(i)
	closer.closeSignal = make(chan struct{})
	return closer
}

// Close 上游通知下游协程进行资源回收，并等待协程通知回收完毕
func (c *Closer) Close() {
	close(c.closeSignal)
	c.waiting.Wait()
}

// Done 标示协程已经完成资源回收，通知上游正式关闭
func (c *Closer) Done() {
	c.waiting.Done()
}

// Wait 返回关闭信号
func (c *Closer) Wait() chan struct{} {
	return c.closeSignal
}

// Add 添加wait 计数
func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}
