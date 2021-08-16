package utils

import "errors"

// NotFoundKey 找不到key
var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")

)
// Panic 如果err 不为nil 则panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
