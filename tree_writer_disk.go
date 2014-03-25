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
	"bytes"
	"io/ioutil"
	"os"

	"github.com/mschoch/mergesort"
)

// FIXME this isn't really MB right? its a count of items, not their size
const ID_SORT_CHUNK_SIZE = (10 * 1024 * 1024) // 10MB. Make tuneable?

type OnDiskTreeWriter struct {
	keyCompare       btreeKeyComparator
	reduce           reduceFunc
	rereduce         reduceFunc
	reduceContext    interface{}
	unsortedFilePath string
	file             *os.File
}

func NewOnDiskTreeWriter(unsortedFilePath string, keyCompare btreeKeyComparator, reduce, rereduce reduceFunc, reduceContext interface{}) (*OnDiskTreeWriter, error) {
	rv := OnDiskTreeWriter{
		unsortedFilePath: unsortedFilePath,
		keyCompare:       keyCompare,
		reduce:           reduce,
		rereduce:         rereduce,
		reduceContext:    reduceContext,
	}

	var err error
	if unsortedFilePath == "" {
		rv.file, err = ioutil.TempFile("", "tw")
		if err != nil {
			return nil, err
		}
	} else {
		rv.file, err = os.OpenFile(unsortedFilePath, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// seek to the end, in case more items will be added
		_, err = rv.file.Seek(0, os.SEEK_END)
		if err != nil {
			return nil, err
		}
	}

	return &rv, nil
}

func (imt *OnDiskTreeWriter) AddItem(key, value []byte) error {
	buffer := new(bytes.Buffer)
	buffer.Write(encode_raw16(uint16(len(key))))
	buffer.Write(encode_raw32(uint32(len(value))))
	buffer.Write(key)
	buffer.Write(value)
	_, err := imt.file.Write(buffer.Bytes())
	return err
}

func (imt *OnDiskTreeWriter) Sort() error {
	// rewind to beginning
	imt.file.Seek(0, os.SEEK_SET)
	// sort it
	err := mergesort.MergeSort(imt.file, imt.file, readRecord, writeRecord, compareRecords, imt, ID_SORT_CHUNK_SIZE)
	return err
}

func (imt *OnDiskTreeWriter) Write(db *Gouchstore) (*nodePointer, error) {
	targetMr := newBtreeModifyResult(imt.keyCompare, imt.reduce, imt.rereduce, imt.reduceContext, gs_DB_CHUNK_THRESHOLD, gs_DB_CHUNK_THRESHOLD)

	// rewind to beginning
	imt.file.Seek(0, os.SEEK_SET)
	rec, err := readRecord(imt.file, nil)
	for err == nil {
		sr := rec.(sortRecord)
		db.mrPushItem(sr.key, sr.val, targetMr)
		rec, err = readRecord(imt.file, nil)
	}

	newRoot, err := db.completeNewBtree(targetMr)
	if err != nil {
		return nil, err
	}
	return newRoot, nil
}

func (imt *OnDiskTreeWriter) Close() error {
	err := imt.file.Close()
	if err != nil {
		return err
	}
	if imt.unsortedFilePath == "" {
		// if this was a tmp file, remove it, otherwise it is callers responsibility
		err = os.Remove(imt.file.Name())
	}
	return err
}

type sortRecord struct {
	klen uint16
	vlen uint32
	key  []byte
	val  []byte
}

func readRecord(file *os.File, context interface{}) (interface{}, error) {
	klenBytes := make([]byte, 2)
	_, err := file.Read(klenBytes)
	if err != nil {
		return nil, err
	}
	vlenBytes := make([]byte, 4)
	_, err = file.Read(vlenBytes)
	if err != nil {
		return nil, err
	}

	rv := sortRecord{}
	rv.klen = decode_raw16(klenBytes)
	rv.vlen = decode_raw32(vlenBytes)
	rv.key = make([]byte, rv.klen)
	rv.val = make([]byte, rv.vlen)

	_, err = file.Read(rv.key)
	if err != nil {
		return nil, err
	}
	_, err = file.Read(rv.val)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func writeRecord(file *os.File, rec interface{}, context interface{}) error {
	sr := rec.(sortRecord)
	buffer := new(bytes.Buffer)
	buffer.Write(encode_raw16(sr.klen))
	buffer.Write(encode_raw32(sr.vlen))
	buffer.Write(sr.key)
	buffer.Write(sr.val)
	_, err := file.Write(buffer.Bytes())
	return err
}

func compareRecords(rec1, rec2 interface{}, context interface{}) int {
	treeWriter := context.(*OnDiskTreeWriter)
	r1 := rec1.(sortRecord)
	r2 := rec2.(sortRecord)
	return treeWriter.keyCompare(r1.key, r2.key)
}
