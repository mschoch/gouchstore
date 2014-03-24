//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package gouchstore

const gs_BLOCK_SIZE int64 = 4096
const gs_BLOCK_MARKER_SIZE int64 = 1

const (
	gs_BLOCK_DATA    byte = 0
	gs_BLOCK_HEADER  byte = 1
	gs_BLOCK_INVALID byte = 0xff
)

func (g *Gouchstore) seekPreviousBlockFrom(pos int64) (int64, byte, error) {
	pos -= 1 // need to move back at least one byte
	pos -= pos % gs_BLOCK_SIZE
	for ; pos >= 0; pos -= gs_BLOCK_SIZE {
		var err error
		buf := make([]byte, 1)
		n, err := g.ops.ReadAt(g.file, buf, pos)
		if n != 1 || err != nil {
			return -1, gs_BLOCK_INVALID, err
		}
		if buf[0] == gs_BLOCK_HEADER {
			return pos, gs_BLOCK_HEADER, nil
		} else if buf[0] == gs_BLOCK_DATA {
			return pos, gs_BLOCK_DATA, nil
		} else {
			return -1, gs_BLOCK_INVALID, nil
		}
	}
	return -1, gs_BLOCK_INVALID, nil
}

func (g *Gouchstore) seekLastHeaderBlockFrom(pos int64) (int64, error) {
	var blockType byte
	var err error
	for pos, blockType, err = g.seekPreviousBlockFrom(pos); blockType != gs_BLOCK_HEADER; pos, blockType, err = g.seekPreviousBlockFrom(pos) {
		if err != nil {
			return -1, err
		}
	}
	return pos, nil
}

// this is just like os.File.ReadAt() except that if your read
// crosses a block boundary, the block marker is removed
func (g *Gouchstore) readAt(buf []byte, pos int64) (int64, error) {
	bytesReadSoFar := int64(0)
	bytesSkipped := int64(0)
	numBytesToRead := int64(len(buf))
	readOffset := pos
	for numBytesToRead > 0 {
		var err error
		bytesTillNextBlock := gs_BLOCK_SIZE - (readOffset % gs_BLOCK_SIZE)
		if bytesTillNextBlock == gs_BLOCK_SIZE {
			readOffset++
			bytesTillNextBlock--
			bytesSkipped++
		}
		bytesToReadThisPass := bytesTillNextBlock
		if bytesToReadThisPass > numBytesToRead {
			bytesToReadThisPass = numBytesToRead
		}
		n, err := g.ops.ReadAt(g.file, buf[bytesReadSoFar:bytesReadSoFar+bytesToReadThisPass], readOffset)
		if err != nil {
			return -1, err
		}
		readOffset += int64(n)
		bytesReadSoFar += int64(n)
		numBytesToRead -= int64(n)
		if int64(n) < bytesToReadThisPass {
			return bytesReadSoFar, nil
		}
	}
	return bytesReadSoFar + bytesSkipped, nil
}

// writeAt is just like os.File.WriteAt() except that if your write
// crosses a block boundary, the correct block marker in inserted
func (g *Gouchstore) writeAt(buf []byte, pos int64, header bool) (int64, error) {
	var err error
	var bufSize int64 = int64(len(buf))
	var writePos int64 = pos
	var bufPos int64
	var written int
	var blockRemain int64
	var blockPrefix byte = 0x00
	if header {
		blockPrefix = 0x01
	}

	for bufPos < bufSize {
		blockRemain = gs_BLOCK_SIZE - (writePos % gs_BLOCK_SIZE)
		if blockRemain > (bufSize - bufPos) {
			blockRemain = bufSize - bufPos
		}

		if writePos%gs_BLOCK_SIZE == 0 {
			written, err = g.ops.WriteAt(g.file, []byte{blockPrefix}, writePos)
			if err != nil {
				return int64(written), err
			}
			writePos += 1
			continue
		}

		written, err = g.ops.WriteAt(g.file, buf[bufPos:bufPos+blockRemain], writePos)
		if err != nil {
			return int64(written), err
		}
		bufPos += int64(written)
		writePos += int64(written)
	}

	return writePos - pos, nil
}
