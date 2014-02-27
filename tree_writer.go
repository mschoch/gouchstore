package gouchstore

import (
	"sort"
)

type treeWriter interface {
	AddItem(key, value []byte) error
	Sort() error
	Write(db *Gouchstore) (*nodePointer, error)
}

type inMemoryTreeWriter struct {
	keyCompare    btreeKeyComparator
	reduce        reduceFunc
	rereduce      reduceFunc
	reduceContext interface{}
	keys          [][]byte
	vals          [][]byte
}

func newInMemoryTreeWriter(keyCompare btreeKeyComparator, reduce, rereduce reduceFunc, reduceContext interface{}) (*inMemoryTreeWriter, error) {
	return &inMemoryTreeWriter{
		keyCompare:    keyCompare,
		reduce:        reduce,
		rereduce:      rereduce,
		reduceContext: reduceContext,
		keys:          make([][]byte, 0),
		vals:          make([][]byte, 0),
	}, nil
}

func (imt *inMemoryTreeWriter) AddItem(key, value []byte) error {
	imt.keys = append(imt.keys, key)
	imt.vals = append(imt.vals, value)
	return nil
}

func (imt *inMemoryTreeWriter) Sort() error {
	sortedIds := idAndValueList{
		ids:  imt.keys,
		vals: imt.vals,
	}
	sort.Sort(sortedIds)
	return nil
}

func (imt *inMemoryTreeWriter) Write(db *Gouchstore) (*nodePointer, error) {

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
