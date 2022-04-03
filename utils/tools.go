// Copyright 2021 bardcckre-os Project Authors
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

func ValueSize(value []byte) int64 {
	return 0
}

// Copy copies a byte slice and returns the copied slice.
func Copy(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}
