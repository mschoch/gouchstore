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
	"hash/crc32"

	"code.google.com/p/snappy-go/snappy"
)

const gs_CHUNK_LENGTH_SIZE int64 = 4
const gs_CHUNK_CRC_SIZE int64 = 4

// attempt to read a chunk at the specified location
func (g *Gouchstore) readChunkAt(pos int64, header bool) ([]byte, error) {
	// chunk starts with 8 bytes (32bit length, 32bit crc)
	chunkPrefix := make([]byte, gs_CHUNK_LENGTH_SIZE+gs_CHUNK_CRC_SIZE)
	n, err := g.readAt(chunkPrefix, pos)
	if err != nil {
		return nil, err
	}
	if n < gs_CHUNK_LENGTH_SIZE+gs_CHUNK_CRC_SIZE {
		return nil, gs_ERROR_INVALID_CHUNK_SHORT_PREFIX
	}

	size := decode_raw31(chunkPrefix[0:gs_CHUNK_LENGTH_SIZE])
	crc := decode_raw32(chunkPrefix[gs_CHUNK_LENGTH_SIZE : gs_CHUNK_LENGTH_SIZE+gs_CHUNK_CRC_SIZE])

	// size should at least be the size of the length field + 1
	if size < uint32(gs_CHUNK_LENGTH_SIZE+1) {
		return nil, gs_ERROR_INVALID_CHUNK_SIZE_TOO_SMALL
	}
	if header {
		size -= uint32(gs_CHUNK_LENGTH_SIZE) // headers include the length of the hash, data does not
	}

	data := make([]byte, size)
	pos += gs_CHUNK_LENGTH_SIZE + gs_CHUNK_CRC_SIZE
	if header {
		pos += gs_BLOCK_MARKER_SIZE // for headers we also skip over the marker
	}
	n, err = g.readAt(data, pos)
	if uint32(n) < size {
		return nil, gs_ERROR_INVALID_CHUNK_DATA_LESS_THAN_SIZE
	}

	// validate crc
	actualCRC := crc32.ChecksumIEEE(data)
	if actualCRC != crc {
		return nil, gs_ERROR_INVALID_CHUNK_BAD_CRC
	}

	return data, nil
}

func (g *Gouchstore) readCompressedDataChunkAt(pos int64) ([]byte, error) {
	chunk, err := g.readChunkAt(pos, false)
	if err != nil {
		return nil, err
	}
	decompressedChunk, err := snappy.Decode(nil, chunk)
	if err != nil {
		return nil, err
	}
	return decompressedChunk, nil
}
