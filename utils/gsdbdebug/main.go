package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/mschoch/gouchstore"
)

type DocumentInfo struct {
	ID           string `json:"id"`           // document identifier
	Seq          uint64 `json:"seq"`          // sequence number in database
	Rev          uint64 `json:"rev"`          // revision number of document
	RevMeta      []byte `json:"revMeta"`      // additional revision meta-data (uninterpreted by Gouchstore)
	ContentMeta  uint8  `json:"contentMeta"`  // content meta-data flags
	Deleted      bool   `json:"deleted"`      // is the revision deleted?
	Size         uint64 `json:"size"`         // size of document data in bytes
	bodyPosition uint64 `json:"bodyPosition"` // byte offset of document body in file
}

var printRawBytes = flag.Bool("printRaw", false, "print raw bytes")
var readLargeChunk = flag.Bool("readLargeChunk", false, "allow reading large chunks")
var indexType = flag.Int("indexType", -1, "index type -1 guess, 0 id, 1 seq, 2 local")

func main() {

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Must specify path to a gouchstore compatible file")
		return
	}
	db, err := gouchstore.Open(flag.Args()[0], gouchstore.OPEN_RDONLY)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	if flag.NArg() < 2 {
		dbInfo, err := db.DatabaseInfo()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("Last valid header found at: %#x\n", dbInfo.HeaderPosition)
		return
	}

	offsetAddress, err := strconv.ParseInt(flag.Arg(1), 0, 64)
	if err != nil {
		fmt.Println(err)
	}

	err = db.DebugAddress(os.Stdout, offsetAddress, *printRawBytes, *readLargeChunk, *indexType)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%v", err)
	}
}
