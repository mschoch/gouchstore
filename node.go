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
	"fmt"
)

const gs_BTREE_INTERIOR byte = 0
const gs_BTREE_LEAF byte = 1

const gs_INDEX_TYPE_BY_ID int = 0
const gs_INDEX_TYPE_BY_SEQ int = 1
const gs_INDEX_TYPE_LOCAL_DOCS int = 2

const gs_KEY_VALUE_LEN int = 5

type node struct {
	// interior nodes will have this
	pointers []*nodePointer
	// leaf nodes will have this
	documents []*DocumentInfo
}

func (n *node) String() string {
	var rv string
	if n.pointers != nil {
		rv = "Interior Node: [\n"
		for i, p := range n.pointers {
			if i != 0 {
				rv += ",\n"
			}
			rv += fmt.Sprintf("%v", p)
		}
		rv += "\n]\n"
	} else {
		rv = "Leaf Node: [\n"
		for i, d := range n.documents {
			if i != 0 {
				rv += ",\n"
			}
			rv += fmt.Sprintf("%v", d)
		}
		rv += "\n]\n"
	}
	return rv
}

func newInteriorNode() *node {
	return &node{
		pointers: make([]*nodePointer, 0),
	}
}

func newLeafNode() *node {
	return &node{
		documents: make([]*DocumentInfo, 0),
	}
}

type nodePointer struct {
	key          []byte
	pointer      uint64
	reducedValue []byte
	subtreeSize  uint64
}

func (np *nodePointer) encodeRoot() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw48(np.pointer))
	buf.Write(encode_raw48(np.subtreeSize))
	buf.Write(np.reducedValue)
	return buf.Bytes()
}

func (np *nodePointer) encode() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw48(np.pointer))
	buf.Write(encode_raw48(np.subtreeSize))
	buf.Write(encode_raw16(uint16(len(np.reducedValue))))
	buf.Write(np.reducedValue)
	return buf.Bytes()
}

func decodeRootNodePointer(data []byte) *nodePointer {
	n := nodePointer{}
	n.pointer = decode_raw48(data[0:6])
	n.subtreeSize = decode_raw48(data[6:12])
	n.reducedValue = data[gs_ROOT_BASE_SIZE:]
	return &n
}

func decodeNodePointer(data []byte) *nodePointer {
	n := nodePointer{}
	n.pointer = decode_raw48(data[0:6])
	n.subtreeSize = decode_raw48(data[6:12])
	reduceValueSize := decode_raw16(data[12:14])
	n.reducedValue = data[14 : 14+reduceValueSize]
	return &n
}

func (np *nodePointer) String() string {
	if np.key == nil {
		return fmt.Sprintf("Root Pointer: %d Subtree Size: %d ReduceValue: % x", np.pointer, np.subtreeSize, np.reducedValue)
	}
	if matchLikelyKey.Match(np.key) {
		return fmt.Sprintf("Key: '%s' (% x) Pointer: %d Subtree Size: %d ReduceValue: % x", np.key, np.key, np.pointer, np.subtreeSize, np.reducedValue)
	} else {
		return fmt.Sprintf("Key: (% x) Pointer: %d Subtree Size: %d ReduceValue: % x", np.key, np.pointer, np.subtreeSize, np.reducedValue)
	}
}

func decodeInteriorBtreeNode(nodeData []byte, indexType int) (*node, error) {
	bufPos := 1
	resultNode := newInteriorNode()
	for bufPos < len(nodeData) {
		key, value, end := decodeKeyValue(nodeData, bufPos)
		valueNodePointer := decodeNodePointer(value)
		valueNodePointer.key = key
		resultNode.pointers = append(resultNode.pointers, valueNodePointer)
		bufPos = end
	}
	return resultNode, nil
}

func decodeLeafBtreeNode(nodeData []byte, indexType int) (*node, error) {
	bufPos := 1
	resultNode := newLeafNode()
	for bufPos < len(nodeData) {
		key, value, end := decodeKeyValue(nodeData, bufPos)
		docinfo := DocumentInfo{}
		if indexType == gs_INDEX_TYPE_BY_ID {
			docinfo.ID = string(key)
			decodeByIdValue(&docinfo, value)
		} else if indexType == gs_INDEX_TYPE_BY_SEQ {
			docinfo.Seq = decode_raw48(key)
			decodeBySeqValue(&docinfo, value)
		}

		resultNode.documents = append(resultNode.documents, &docinfo)
		bufPos = end
	}
	return resultNode, nil
}

func decodeByIdValue(docinfo *DocumentInfo, value []byte) {
	docinfo.Seq = decode_raw48(value[0:6])
	docinfo.Size = uint64(decode_raw32(value[6:10]))
	docinfo.Deleted, docinfo.bodyPosition = decode_raw_1_47_split(value[10:16])
	docinfo.Rev = decode_raw48(value[16:22])
	docinfo.ContentMeta = decode_raw08(value[22:23])
	docinfo.RevMeta = value[23:]
}

func (d DocumentInfo) encodeById() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw48(d.Seq))
	buf.Write(encode_raw32(d.Size))
	buf.Write(encode_raw_1_47_split(d.Deleted, d.bodyPosition))
	buf.Write(encode_raw48(d.Rev))
	buf.Write(encode_raw08(d.ContentMeta))
	buf.Write(d.RevMeta)
	return buf.Bytes()
}

func decodeBySeqValue(docinfo *DocumentInfo, value []byte) {
	idSize, docSize := decode_raw_12_28_split(value[0:5])
	docinfo.Size = uint64(docSize)
	docinfo.Deleted, docinfo.bodyPosition = decode_raw_1_47_split(value[5:12])
	docinfo.Rev = decode_raw48(value[11:17])
	docinfo.ContentMeta = decode_raw08(value[17:18])
	docinfo.ID = string(value[18 : 18+idSize])
	docinfo.RevMeta = value[18+idSize:]
}

func (d DocumentInfo) encodeBySeq() []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw_12_28_split(uint32(len(d.ID)), uint32(d.Size)))
	buf.Write(encode_raw_1_47_split(d.Deleted, d.bodyPosition))
	buf.Write(encode_raw48(d.Rev))
	buf.Write(encode_raw08(d.ContentMeta))
	buf.Write([]byte(d.ID))
	buf.Write(d.RevMeta)
	return buf.Bytes()
}

func decodeKeyValue(nodeData []byte, bufPos int) ([]byte, []byte, int) {
	keyLength, valueLength := decode_raw_12_28_split(nodeData[bufPos : bufPos+5])
	keyStart := bufPos + 5
	keyEnd := keyStart + int(keyLength)
	key := nodeData[keyStart:keyEnd]
	valueStart := keyEnd
	valueEnd := valueStart + int(valueLength)
	value := nodeData[valueStart:valueEnd]
	return key, value, valueEnd
}

func encodeKeyValue(key, value []byte) []byte {
	buf := new(bytes.Buffer)
	keyLength := len(key)
	valueLength := len(value)
	buf.Write(encode_raw_12_28_split(uint32(keyLength), uint32(valueLength)))
	buf.Write(key)
	buf.Write(value)
	return buf.Bytes()
}

type keyValueIterator struct {
	data []byte
	pos  int
}

func newKeyValueIterator(data []byte) *keyValueIterator {
	return &keyValueIterator{
		data: data,
	}
}

func (kvi *keyValueIterator) Next() ([]byte, []byte) {
	if kvi.pos < len(kvi.data) {
		key, value, end := decodeKeyValue(kvi.data, kvi.pos)
		kvi.pos = end
		return key, value
	}
	return nil, nil
}
