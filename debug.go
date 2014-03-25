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
	"io"
	"regexp"
)

// a key which is all printable characters is likely to be in the byId index
var matchLikelyKey = regexp.MustCompile(`^[[:print:]]*$`)

func (g *Gouchstore) DebugAddress(w io.Writer, offsetAddress int64, printRawBytes, readLargeChunk bool, indexType int) error {
	if offsetAddress%4096 == 0 {
		fmt.Fprintln(w, "Address is on a 4096 byte boundary...")
		first := make([]byte, 1)
		_, err := g.readAt(first, offsetAddress)
		if err != nil {
			return err
		}
		if first[0] == 0 {
			fmt.Fprintln(w, "Appears to be a header...")
			chunk, err := g.readChunkAt(offsetAddress, true)
			if err != nil {
				return err
			}
			fmt.Fprintln(w, "Header Found!")
			if printRawBytes {
				fmt.Fprintf(w, "Header bytes % x\n", chunk)
			}
			h, err := newHeaderFromBytes(chunk)
			if err != nil {
				return err
			}
			fmt.Fprint(w, h)

		} else {
			fmt.Fprintf(w, "Does not appear to be a header (% x)\n", first[0])
		}
	} else {
		fmt.Fprintln(w, "Trying to read compressed chunk...")
		more := make([]byte, 8)
		g.readAt(more, offsetAddress)
		chunkSize := decode_raw31(more[0:4])
		if chunkSize > 4096 && !readLargeChunk {
			return fmt.Errorf("Chunk appears to be too large (%d), check the address or use --readLargeChunk to proceed\n", chunkSize)
		}
		chunk, err := g.readCompressedDataChunkAt(offsetAddress)
		if err != nil {
			return err
		}
		if printRawBytes {
			fmt.Fprintf(w, "raw chunk data: % x\n", chunk)
		}
		if chunk[0] == gs_BTREE_INTERIOR {
			fmt.Fprintln(w, "Appears to be an interior node...")
			node, err := decodeInteriorBtreeNode(chunk, indexType)
			if err != nil {
				return err
			}
			fmt.Fprintln(w, "Interior node found!")
			fmt.Fprintf(w, "%v", node)
		} else if chunk[0] == gs_BTREE_LEAF {
			fmt.Fprintln(w, "Appears to be a leaf node...")
			if indexType == -1 {
				// try to guess the index type, this is just heuristic and will be wrong sometimes
				k, _, _ := decodeKeyValue(chunk, 1)
				if matchLikelyKey.Match(k) {
					indexType = 0
					fmt.Fprintln(w, "Guessing this node is in the byId index")
				} else {
					indexType = 1
					fmt.Fprintln(w, "Guessing this node is in the bySeq index")
				}
			}

			node, err := decodeLeafBtreeNode(chunk, indexType)
			if err != nil {
				return err
			}
			fmt.Fprintln(w, "Leaf node found!")
			fmt.Fprintf(w, "%v", node)
		} else {
			fmt.Fprintln(w, "Assuming data chunk!")
		}
	}
	return nil
}
