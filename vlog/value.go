package vlog

import "github.com/hardcore-os/corekv/codec"

type ValuePtr struct {
}

// NewValuePtr
func NewValuePtr(entry *codec.Entry) *ValuePtr {
	return &ValuePtr{}
}
