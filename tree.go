//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package gouchstore

import ()

type lookupContext struct {
	gouchstore           *Gouchstore
	documentInfoCallback DocumentInfoCallback
	walkTreeCallback     WalkTreeCallback
	indexType            int
	depth                int
	callbackContext      interface{}
}

type lookupRequest struct {
	gouchstore      *Gouchstore
	compare         btreeKeyComparator
	keys            [][]byte
	fold            bool
	inFold          bool
	fetchCallback   callback
	nodeCallback    callback
	callbackContext interface{}
}

type callback func(req *lookupRequest, key []byte, value []byte) error

func (g *Gouchstore) btreeLookupInner(req *lookupRequest, diskPos uint64, current, end int) error {
	nodeData, err := g.readCompressedDataChunkAt(int64(diskPos))
	if err != nil {
		return err
	}

	if nodeData[0] == gs_BTREE_INTERIOR {
		kvIterator := newKeyValueIterator(nodeData[1:])
		for k, v := kvIterator.Next(); k != nil && current < end; k, v = kvIterator.Next() {
			cmp := req.compare(k, req.keys[current])
			if cmp >= 0 {
				if req.fold {
					req.inFold = true
				}

				// Descend into the pointed to node.
				// with all keys < item key.
				lastItem := current + 1
				for lastItem < end && req.compare(k, req.keys[lastItem]) >= 0 {
					lastItem++
				}

				if req.nodeCallback != nil {
					err = req.nodeCallback(req, k, v)
					if err != nil {
						return err
					}
				}

				valNodePointer := decodeNodePointer(v)
				err = g.btreeLookupInner(req, valNodePointer.pointer, current, lastItem)
				if err != nil {
					return err
				}

				if !req.inFold {
					current = lastItem
				}
				if req.nodeCallback != nil {
					err = req.nodeCallback(req, nil, nil)
					if err != nil {
						return err
					}
				}
			}
		}
	} else if nodeData[0] == gs_BTREE_LEAF {
		kvIterator := newKeyValueIterator(nodeData[1:])
		for k, v := kvIterator.Next(); k != nil && current < end; k, v = kvIterator.Next() {
			cmp := req.compare(k, req.keys[current])
			if cmp >= 0 && req.fold && !req.inFold {
				req.inFold = true
			} else if req.inFold && (current+1) < end && req.compare(k, req.keys[current+1]) > 0 {
				//We've hit a key past the end of our range.
				req.inFold = false
				req.fold = false
				current = end
			}

			if cmp == 0 || (cmp > 0 && req.inFold) {
				// Found
				err = req.fetchCallback(req, k, v)
				if err != nil {
					return err
				}

				if !req.inFold {
					current++
				}
			}
		}
	}

	//Any remaining items are not found.
	for current < end && !req.fold {
		err = req.fetchCallback(req, req.keys[current], nil)
		current++
	}

	return nil
}

func (g *Gouchstore) btreeLookup(req *lookupRequest, rootPointer uint64) error {
	req.inFold = false
	return g.btreeLookupInner(req, rootPointer, 0, len(req.keys))
}
