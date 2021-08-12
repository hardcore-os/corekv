package utils

import "strings"

// FID 根据file name 获取其fid
func FID(name string) string {
	return strings.Split(name, ".")[0]
}
