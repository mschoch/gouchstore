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

func (g *Gouchstore) btree_lookup(np *nodePointer, indexType int, current *int, ids [][]byte, compare btreeKeyComparator, cb DocumentInfoCallback, userContext interface{}) error {
	nodeData, err := g.readCompressedDataChunkAt(int64(np.pointer))
	if err != nil {
		return err
	}

	if nodeData[0] == gs_BTREE_INTERIOR {
		node, err := decodeInteriorBtreeNode(nodeData, indexType)
		if err != nil {
			return err
		}
		for _, pointer := range node.pointers {
			c := ids[*current]
			if compare(pointer.key, c) >= 0 {
				// we need to descend
				g.btree_lookup(pointer, indexType, current, ids, compare, cb, userContext)
				if *current == len(ids) {
					// all done
					return nil
				}
			}
		}
		return nil
	} else if nodeData[0] == gs_BTREE_LEAF {
		node, err := decodeLeafBtreeNode(nodeData, indexType)
		if err != nil {
			return err
		}
		for _, docInfo := range node.documents {
			var comp int
			if indexType == gs_INDEX_TYPE_BY_ID {
				comp = compare([]byte(docInfo.ID), ids[*current])
			} else {
				comp = compare(encode_raw48(docInfo.Seq), ids[*current])
			}
			if comp == 0 {
				cb(g, docInfo, userContext)
				*current++
				if *current == len(ids) {
					// all done
					return nil
				}
			}
		}
		return nil
	} else {
		return gs_ERROR_INVALID_BTREE_NODE_TYPE
	}
}

func (g *Gouchstore) btree_range(np *nodePointer, depth, indexType int, active *int, startId, endId []byte, compare btreeKeyComparator, wtcb WalkTreeCallback, userContext interface{}) error {
	nodeData, err := g.readCompressedDataChunkAt(int64(np.pointer))
	if err != nil {
		return err
	}

	if nodeData[0] == gs_BTREE_INTERIOR {
		node, err := decodeInteriorBtreeNode(nodeData, indexType)
		if err != nil {
			return err
		}
		for _, pointer := range node.pointers {
			wtcb(g, depth, nil, pointer.key, pointer.subtreeSize, pointer.reducedValue, userContext)
			if *active != 2 && compare(pointer.key, startId) >= 0 {
				// we need to descend
				g.btree_range(pointer, depth+1, indexType, active, startId, endId, compare, wtcb, userContext)
				if *active == 2 {
					// all done
					return nil
				}
			}
		}
		return nil
	} else if nodeData[0] == gs_BTREE_LEAF {
		node, err := decodeLeafBtreeNode(nodeData, indexType)
		if err != nil {
			return err
		}
		for _, docInfo := range node.documents {
			var comp int
			if *active == 0 {
				if indexType == gs_INDEX_TYPE_BY_ID {
					comp = compare([]byte(docInfo.ID), startId)
				} else {
					comp = compare(encode_raw48(docInfo.Seq), startId)
				}
				if comp >= 0 {
					wtcb(g, depth, docInfo, nil, 0, nil, userContext)
					*active = 1
				}
			} else if *active == 1 && len(endId) != 0 {
				if indexType == gs_INDEX_TYPE_BY_ID {
					comp = compare([]byte(docInfo.ID), endId)
				} else {
					comp = compare(encode_raw48(docInfo.Seq), endId)
				}
				if comp <= 0 {
					wtcb(g, depth, docInfo, nil, 0, nil, userContext)
				}
				if comp >= 0 {
					// all done
					*active = 2
					return nil
				}
			} else if *active == 1 && len(endId) == 0 {
				wtcb(g, depth, docInfo, nil, 0, nil, userContext)
			}
		}
		return nil
	} else {
		return gs_ERROR_INVALID_BTREE_NODE_TYPE
	}
}
