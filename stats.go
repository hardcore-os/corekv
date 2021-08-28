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
		case <-s.closer.Wait():
			break
		}
		// stats logic...
	}
}

// NewStats
func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser(1)
	s.EntryNum = 1 // 这里直接写1
	return s
}
