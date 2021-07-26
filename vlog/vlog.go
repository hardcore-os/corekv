package vlog

import "github.com/hardcore-os/corekv/utils"

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
	v.closer = utils.NewCloser(1)
	return v
}

// StartGC
func (v *VLog) StartGC() {
	defer v.closer.Done()
	for {
		select {
		case <-v.closer.Wait():
		}
		// gc logic...
	}
}

// Set
func (v *VLog) Set(ptr *ValuePtr) error {
	return nil
}
