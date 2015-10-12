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

	"github.com/golang/snappy"
)

type BaseGouchOps struct{}

func NewBaseGouchOps() *BaseGouchOps {
	return &BaseGouchOps{}
}

func (g *BaseGouchOps) OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, flag, perm)
}

func (g *BaseGouchOps) ReadAt(f *os.File, b []byte, off int64) (n int, err error) {
	return f.ReadAt(b, off)
}

func (g *BaseGouchOps) WriteAt(f *os.File, b []byte, off int64) (n int, err error) {
	return f.WriteAt(b, off)
}

func (g *BaseGouchOps) GotoEOF(f *os.File) (ret int64, err error) {
	return f.Seek(0, os.SEEK_END)
}

func (g *BaseGouchOps) Sync(f *os.File) error {
	return f.Sync()
}

func (g *BaseGouchOps) CompactionTreeWriter(keyCompare btreeKeyComparator, reduce, rereduce reduceFunc, reduceContext interface{}) (TreeWriter, error) {
	return NewOnDiskTreeWriter("", keyCompare, reduce, rereduce, reduceContext)
}

func (g *BaseGouchOps) SnappyEncode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func (g *BaseGouchOps) SnappyDecode(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func (g *BaseGouchOps) Close(f *os.File) error {
	return f.Close()
}
