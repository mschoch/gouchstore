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
	"sort"
)

type TreeWriter interface {
	AddItem(key, value []byte) error
	Sort() error
	Write(db *Gouchstore) (*nodePointer, error)
	Close() error
}

type InMemoryTreeWriter struct {
	keyCompare    btreeKeyComparator
	reduce        reduceFunc
	rereduce      reduceFunc
	reduceContext interface{}
	keys          [][]byte
	vals          [][]byte
}

func NewInMemoryTreeWriter(keyCompare btreeKeyComparator, reduce, rereduce reduceFunc, reduceContext interface{}) (*InMemoryTreeWriter, error) {
	return &InMemoryTreeWriter{
		keyCompare:    keyCompare,
		reduce:        reduce,
		rereduce:      rereduce,
		reduceContext: reduceContext,
		keys:          make([][]byte, 0),
		vals:          make([][]byte, 0),
	}, nil
}

func (imt *InMemoryTreeWriter) AddItem(key, value []byte) error {
	imt.keys = append(imt.keys, key)
	imt.vals = append(imt.vals, value)
	return nil
}

func (imt *InMemoryTreeWriter) Sort() error {
	sortedIds := idAndValueList{
		ids:  imt.keys,
		vals: imt.vals,
	}
	sort.Sort(sortedIds)
	return nil
}

func (imt *InMemoryTreeWriter) Write(db *Gouchstore) (*nodePointer, error) {
	targetMr := newBtreeModifyResult(imt.keyCompare, imt.reduce, imt.rereduce, imt.reduceContext, gs_DB_CHUNK_THRESHOLD, gs_DB_CHUNK_THRESHOLD)

	for i, key := range imt.keys {
		value := imt.vals[i]
		db.mrPushItem(key, value, targetMr)
	}

	newRoot, err := db.completeNewBtree(targetMr)
	if err != nil {
		return nil, err
	}
	return newRoot, nil
}

func (imt *InMemoryTreeWriter) Close() error {
	return nil
}
