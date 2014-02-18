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
	"fmt"
)

const gs_ROOT_BASE_SIZE = 12
const gs_HEADER_BASE_SIZE int = 25

type header struct {
	diskVersion   uint64
	updateSeq     uint64
	purgeSeq      uint64
	purgePtr      uint64
	byIdRoot      *nodePointer
	bySeqRoot     *nodePointer
	localDocsRoot *nodePointer
	position      uint64
}

func (h *header) String() string {
	rv := fmt.Sprintf("Disk Version: %d\n", h.diskVersion)
	rv += fmt.Sprintf("Update Seq: %d\n", h.updateSeq)
	rv += fmt.Sprintf("Purge Seq: %d\n", h.purgeSeq)
	rv += fmt.Sprintf("Purge Pointer: %d\n", h.purgePtr)
	rv += fmt.Sprintf("By ID Root: %v\n", h.byIdRoot)
	rv += fmt.Sprintf("By Seq Root: %v\n", h.bySeqRoot)
	rv += fmt.Sprintf("Local Docs Root: %v\n", h.localDocsRoot)
	return rv
}

func newHeader(data []byte) (*header, error) {

	rv := header{}

	rv.diskVersion = uint64(decode_raw08(data[0:1]))
	rv.updateSeq = decode_raw48(data[1:7])
	rv.purgeSeq = decode_raw48(data[7:13])
	rv.purgePtr = decode_raw48(data[13:19])

	bySeqRootSize := decode_raw16(data[19:21])
	byIdRootSize := decode_raw16(data[21:23])
	localDocRootSize := decode_raw16(data[23:25])

	if len(data) != gs_HEADER_BASE_SIZE+int(bySeqRootSize+byIdRootSize+localDocRootSize) {
		return nil, gs_ERROR_INVALID_HEADER_BAD_SIZE
	}

	pointerOffset := gs_HEADER_BASE_SIZE
	if bySeqRootSize > 0 {
		rv.bySeqRoot = decodeRootNodePointer(data[pointerOffset : pointerOffset+int(bySeqRootSize)])
	}
	pointerOffset += int(bySeqRootSize)
	if byIdRootSize > 0 {
		rv.byIdRoot = decodeRootNodePointer(data[pointerOffset : pointerOffset+int(byIdRootSize)])
	}
	pointerOffset += int(byIdRootSize)
	if localDocRootSize > 0 {
		rv.localDocsRoot = decodeRootNodePointer(data[pointerOffset : pointerOffset+int(localDocRootSize)])
	}

	return &rv, nil
}

func (g *Gouchstore) readHeaderAt(pos int64) (*header, error) {
	chunk, err := g.readChunkAt(pos, true)
	if err != nil {
		return nil, err
	}
	header, err := newHeader(chunk)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (g *Gouchstore) findLastHeader() error {
	headerPos, err := g.seekLastHeaderBlock()
	if err != nil {
		return err
	}
	header, err := g.readHeaderAt(headerPos)
	if err != nil {
		return err
	}
	header.position = uint64(headerPos)
	g.header = header
	return nil
}
