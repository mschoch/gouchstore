//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package gouchstore

import (
	"os"
)

// GouchOps an interface for plugging in differentl implementations
// of some common low-level operations
type GouchOps interface {
	OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error)
	ReadAt(f *os.File, b []byte, off int64) (n int, err error)
	WriteAt(f *os.File, b []byte, off int64) (n int, err error)
	GotoEOF(f *os.File) (ret int64, err error)
	Sync(f *os.File) error
	CompactionTreeWriter(keyCompare btreeKeyComparator, reduce, rereduce reduceFunc, reduceContext interface{}) (TreeWriter, error)
	SnappyEncode(dst, src []byte) []byte
	SnappyDecode(dst, src []byte) ([]byte, error)
	Close(f *os.File) error
}
