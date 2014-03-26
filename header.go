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

const gs_DISK_VERSION = 11
const gs_ROOT_BASE_SIZE = 12
const gs_HEADER_BASE_SIZE int64 = 25

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

func (h *header) toBytes() []byte {
	buf := new(bytes.Buffer)

	buf.Write(encode_raw08(h.diskVersion))
	buf.Write(encode_raw48(h.updateSeq))
	buf.Write(encode_raw48(h.purgeSeq))
	buf.Write(encode_raw48(h.purgePtr))

	var bySeqBytes, byIdBytes, localDocsBytes []byte

	if h.bySeqRoot != nil {
		bySeqBytes = h.bySeqRoot.encodeRoot()
	}
	if h.byIdRoot != nil {
		byIdBytes = h.byIdRoot.encodeRoot()
	}
	if h.localDocsRoot != nil {
		localDocsBytes = h.localDocsRoot.encodeRoot()
	}

	buf.Write(encode_raw16(uint16(len(bySeqBytes))))
	buf.Write(encode_raw16(uint16(len(byIdBytes))))
	buf.Write(encode_raw16(uint16(len(localDocsBytes))))

	if bySeqBytes != nil {
		buf.Write(bySeqBytes)
	}
	if byIdBytes != nil {
		buf.Write(byIdBytes)
	}
	if localDocsBytes != nil {
		buf.Write(localDocsBytes)
	}

	return buf.Bytes()
}

func (h *header) String() string {
	rv := fmt.Sprintf("Disk Version: %d (0x%x)\n", h.diskVersion, h.diskVersion)
	rv += fmt.Sprintf("Update Seq: %d\n", h.updateSeq)
	rv += fmt.Sprintf("Purge Seq: %d\n", h.purgeSeq)
	rv += fmt.Sprintf("Purge Pointer: %d (0x%x)\n", h.purgePtr, h.purgePtr)
	if h.bySeqRoot != nil {
		rv += fmt.Sprintf("By Sequence Pointer: %d (0x%x)\n", h.bySeqRoot.pointer, h.bySeqRoot.pointer)
		rv += fmt.Sprintf("By Sequence Subtree Size: %d (0x%x)\n", h.bySeqRoot.subtreeSize, h.bySeqRoot.subtreeSize)
		count := decode_raw40(h.bySeqRoot.reducedValue)
		rv += fmt.Sprintf("By Sequence Reduced Count: %d\n", count)
	} else {
		rv += fmt.Sprintf("By Sequence Pointer: nil\n")
	}
	if h.byIdRoot != nil {
		rv += fmt.Sprintf("By ID Pointer: %d (0x%x)\n", h.byIdRoot.pointer, h.byIdRoot.pointer)
		rv += fmt.Sprintf("By ID Subtree Size: %d (0x%x)\n", h.byIdRoot.subtreeSize, h.byIdRoot.subtreeSize)
		notDeleted, deleted, size := decodeByIdReduce(h.byIdRoot.reducedValue)
		rv += fmt.Sprintf("By ID Reduced Document Count: %d\n", notDeleted)
		rv += fmt.Sprintf("By ID Reduced Deleted Document Count: %d\n", deleted)
		rv += fmt.Sprintf("By ID Reduced Size: %d\n", size)
	} else {
		rv += fmt.Sprintf("By ID Pointer: nil\n")
	}
	if h.localDocsRoot != nil {
		rv += fmt.Sprintf("Local Docs Pointer: %d (0x%x)\n", h.localDocsRoot.pointer, h.localDocsRoot.pointer)
		rv += fmt.Sprintf("Local Docs Subtree Size: %d (0x%x)\n", h.localDocsRoot.subtreeSize, h.localDocsRoot.subtreeSize)
	} else {
		rv += fmt.Sprintf("Local Docs Pointer: nil\n")
	}
	return rv
}

func newHeader() *header {
	rv := header{}
	rv.diskVersion = gs_DISK_VERSION
	return &rv
}

func newHeaderFromBytes(data []byte) (*header, error) {

	rv := header{}

	rv.diskVersion = uint64(decode_raw08(data[0:1]))
	rv.updateSeq = decode_raw48(data[1:7])
	rv.purgeSeq = decode_raw48(data[7:13])
	rv.purgePtr = decode_raw48(data[13:19])

	bySeqRootSize := decode_raw16(data[19:21])
	byIdRootSize := decode_raw16(data[21:23])
	localDocRootSize := decode_raw16(data[23:25])

	if len(data) != int(gs_HEADER_BASE_SIZE)+int(bySeqRootSize+byIdRootSize+localDocRootSize) {
		return nil, gs_ERROR_INVALID_HEADER_BAD_SIZE
	}

	pointerOffset := int(gs_HEADER_BASE_SIZE)
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
	header, err := newHeaderFromBytes(chunk)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (g *Gouchstore) findLastHeader() error {
	pos := g.pos
	var h *header
	var err error = fmt.Errorf("start")
	var headerPos int64
	for h == nil && err != nil {
		headerPos, err = g.seekLastHeaderBlockFrom(pos)
		if err != nil {
			return err
		}
		h, err = g.readHeaderAt(headerPos)
		if err != nil {
			pos = headerPos - 1
		}
	}
	h.position = uint64(headerPos)
	g.header = h
	return nil
}

func (g *Gouchstore) writeHeader(h *header) error {
	headerBytes := h.toBytes()
	_, _, err := g.writeChunk(headerBytes, true)
	if err != nil {
		return err
	}
	return nil
}
