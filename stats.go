package corekv

import "github.com/hardcore-os/corekv/utils"

type Stats struct {
	closer *utils.Closer
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
		}
		// stats logic...
	}
}

// NewStats
func newStats(opt *Options) *Stats {
	s := &Stats{}
	s.closer = utils.NewCloser(1)
	return s
}
