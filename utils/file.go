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

import (
	"strconv"
	"strings"
)

// FID 根据file name 获取其fid
func FID(name string) uint32 {
	ns := strings.Split(name, "/")
	if len(ns) == 0 {
		return 0
	}
	tableName := ns[len(ns)-1]
	j := 0
	for i := range tableName {
		if tableName[i] != '0'-0 {
			break
		}
		j++
	}
	fidStr := tableName[j:]
	if len(fidStr) == 0 {
		return 0
	}
	ss := strings.Split(fidStr, ".")[0]
	fid, err := strconv.ParseUint(ss, 10, 32)
	Panic(err)
	return uint32(fid)
}
