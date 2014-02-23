package gouchstore

import (
	"fmt"
	"regexp"
)

// a key which is all printable characters is likely to be in the byId index
var matchLikelyKey = regexp.MustCompile(`^[[:print:]]*$`)

func (g *Gouchstore) DebugAddress(offsetAddress int64, printRawBytes, readLargeChunk bool, indexType int) {
	if offsetAddress%4096 == 0 {
		fmt.Println("Address is on a 4096 byte boundary...")
		first := make([]byte, 1)
		_, err := g.readAt(first, offsetAddress)
		if err != nil {
			fmt.Println(err)
			return
		}
		if first[0] == 0 {
			fmt.Println("Appears to be a header...")
			chunk, err := g.readChunkAt(offsetAddress, true)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("Header Found!")
			if printRawBytes {
				fmt.Printf("Header bytes % x\n", chunk)
			}
			h, err := newHeaderFromBytes(chunk)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("Disk Version: %d (0x%x)\n", h.diskVersion, h.diskVersion)
			fmt.Printf("Update Seq: %d\n", h.updateSeq)
			fmt.Printf("Purge Seq: %d\n", h.purgeSeq)
			fmt.Printf("Purge Pointer: %d (0x%x)\n", h.purgePtr, h.purgePtr)
			if h.bySeqRoot != nil {
				fmt.Printf("By Sequence Pointer: %d (0x%x)\n", h.bySeqRoot.pointer, h.bySeqRoot.pointer)
				fmt.Printf("By Sequence Subtree Size: %d (0x%x)\n", h.bySeqRoot.subtreeSize, h.bySeqRoot.subtreeSize)
				count := decode_raw40(h.bySeqRoot.reducedValue)
				fmt.Printf("By Sequence Reduced Count: %d\n", count)
			} else {
				fmt.Printf("By Sequence Pointer: nil\n")
			}
			if h.byIdRoot != nil {
				fmt.Printf("By ID Pointer: %d (0x%x)\n", h.byIdRoot.pointer, h.byIdRoot.pointer)
				fmt.Printf("By ID Subtree Size: %d (0x%x)\n", h.byIdRoot.subtreeSize, h.byIdRoot.subtreeSize)
				notDeleted, deleted, size := decodeByIdReduce(h.byIdRoot.reducedValue)
				fmt.Printf("By ID Reduced Document Count: %d\n", notDeleted)
				fmt.Printf("By ID Reduced Deleted Document Count: %d\n", deleted)
				fmt.Printf("By ID Reduced Size: %d\n", size)
			} else {
				fmt.Printf("By ID Pointer: nil\n")
			}
			if h.localDocsRoot != nil {
				fmt.Printf("Local Docs Pointer: %d (0x%x)\n", h.localDocsRoot.pointer, h.localDocsRoot.pointer)
				fmt.Printf("Local Docs Subtree Size: %d (0x%x)\n", h.localDocsRoot.subtreeSize, h.localDocsRoot.subtreeSize)
			} else {
				fmt.Printf("Local Docs Pointer: nil\n")
			}

		} else {
			fmt.Printf("Does not appear to be a header (% x)\n", first[0])
		}
	} else {
		fmt.Println("Trying to read compressed chunk...")
		more := make([]byte, 8)
		g.readAt(more, offsetAddress)
		chunkSize := decode_raw31(more[0:4])
		if chunkSize > 4096 && !readLargeChunk {
			fmt.Printf("Chunk appears to be too large (%d), check the address or use --readLargeChunk to proceed\n", chunkSize)
			return
		}
		chunk, err := g.readCompressedDataChunkAt(offsetAddress)
		if err != nil {
			fmt.Println(err)
			return
		}
		if printRawBytes {
			fmt.Printf("raw chunk data: % x\n", chunk)
		}
		if chunk[0] == gs_BTREE_INTERIOR {
			fmt.Println("Appears to be an interior node...")
			node, err := decodeInteriorBtreeNode(chunk, indexType)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("Interior node found!")
			fmt.Printf("%v\n", node)
		} else if chunk[0] == gs_BTREE_LEAF {
			fmt.Println("Appears to be a leaf node...")
			if indexType == -1 {
				// try to guess the index type, this is just heuristic and will be wrong sometimes
				k, _, _ := decodeKeyValue(chunk, 1)
				if matchLikelyKey.Match(k) {
					indexType = 0
					fmt.Println("Guessing this node is in the byId index")
				} else {
					indexType = 1
					fmt.Println("Guessing this node is in the bySeq index")
				}
			}

			node, err := decodeLeafBtreeNode(chunk, indexType)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("Leaf node found!")
			fmt.Printf("%v\n", node)
		} else {
			fmt.Println("Assuming data chunk!")
		}
	}
}
