//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/mschoch/gouchstore"
)

var numOps = flag.Int("numOps", 1000, "number of operations to simulate")
var commitEvery = flag.Int("commitEvery", 100, "commit changes every N operations")
var commitAtEnd = flag.Bool("commitAtEnd", true, "commit changes after all operations")

func main() {

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Must specify path to a use")
		return
	}
	db, err := gouchstore.Open(flag.Args()[0], gouchstore.OPEN_CREATE)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	var docCount uint64 = 0
	var deletedCount uint64 = 0
	nextDocId := 0
	docs := make(map[string]*gouchstore.Document)
	deletedDocs := make(map[string]bool)
	docInfos := make(map[string]*gouchstore.DocumentInfo)
	docKeys := make([]string, 0)
	docsBySeq := make(map[uint64]*gouchstore.Document)
	deletedDocsBySeq := make(map[uint64]bool)
	// iterate through this many operations
	for i := 0; i < *numOps; i++ {
		operation := rand.Intn(100)
		if operation < 70 {
			// add doc
			docId := "doc-" + strconv.Itoa(nextDocId)
			doc := &gouchstore.Document{
				ID:   docId,
				Body: []byte(`{"content":123,"filler":"hf7BB83mKJ5APQ0OBqrj1wnYw2mjArEFmkuPaiNIYbBa4cMkOdI/JuTsGZTddqW5cxLYT1md1kWPxwuNUYliPAcrY/eoibeOMJ/3HaBUu88S/unTrR/P8iVMR1RcV0jyVzw475E3ckff8AqomvglH3ij7gsJyrAMmF2TjLlJU2EURrp+Sq5sM3bboeHf7iyuG+LnUL1hKbYju+yxC7AaoWDoiUKB2KlrZrcKwolFQnTKkZcSMEuI9sXsUvdaoq+1MNMsFKnnlQsuricwTTfcFIFQFHlyGrV+ghrljAl7RY6kxNpPtFft8s0x8xYR8Po8SxTLgA/YNEa2ERbHPUCdheAZZ3MDMSeBlv9Sy7UOCgKVH1tW8S9kidJQLwnYoDcDlz1HCxrvvakdyrYPoeohZ5Ub44xo1/2hhO0Pypk11dWbTqO8MV/3Z+BIQWdVWbvx7zrLSG+kzxh1tMBnrluuZWXjT5w/+ZUSBnLU3Ru17TpIGu5I8pp4Xh2vjWgbRsdKHGga7li5tqtO5fb59rhA+8q+lLlBTcL1AR1oZiPL/O7O+2jqHE10gE7CO4GFqfX/iM4LhrxzcnPd3taeiVGp3/zTx1JJKiYn9H8yn7e4/KkpNObag7k+bqrpE64GPWOHvlYXwm7L8kVIesaBfZEwenlOWJgK73IleBzhoYVt6iKgS+MPZqNvQBN/ndXXffoyUoM9dbiV/dYEt7pmP14N3UVqJh3N/7Kz3bXrrQH0msmso1FnfkoZTUnPEK3toXgnub1dEPkbmJnwbiXBaNhHc7KPBnMYH7HlVWN4h7R0tH9u0zgAzemdBkARViJidkYhieig5K/31OfVSVFjbqbdWNtanAU1kSzcqX+O9+3H3kFQxB8Ez7Lo08Zbx2IojmjmVnJUH+oNs0qhYwlrmhH1lK2/TOIdaEASnYsANTxnlJvYWpSbNfpk2jXcyBQCvhaIOO2E8Hq+eRhdMtdqLUwoOtI8+Q0lJVaCxuM4RhVEtvBm7frqC3fm5fCJjYmjNOAF6QGWhRWnkeaEQgA0j40DbvRQypSPGr2y6mLOpsCeELtxXKpm929CngxLzWRsGZWvmCOZNtpH6zqHGmn5PTcm2KyTC1jhoD3maicGvGF/S7MJI5Pftuzp8GVx+EieqONwPZXln6g0L66Md2V8igH+Z134RXMN4mk2j6MGhZiZ4oUokbBZ9WR/QvGUGdxzoyfFnGt1Bh+9uUaydq1l6Lk1OHH3XrRdkexMPT2CFEpX+0JonOtNwaXTmdbclFjJIA9ObfNww0iBfxQ7TZYZSv4O0mHIBXcCkhM2/kqlFbomL3/eJXhJhDaGsc+KoVBQCU4x9wydSiiXGFmskX5gWV37bA=="}`),
			}
			nextDocId++
			docInfo := gouchstore.NewDocumentInfo(docId)
			docInfo.Rev = 1
			err = db.SaveDocument(doc, docInfo)
			if err != nil {
				fmt.Printf("err: %v\n", err)
				return
			}
			docs[docId] = doc
			docsBySeq[docInfo.Seq] = doc
			docInfos[docId] = docInfo
			docKeys = append(docKeys, docId)
			docCount++
		} else if operation < 90 {
			// update doc
			if len(docKeys) > 0 {
				docIdIndexToUpdate := rand.Intn(len(docKeys))
				docIdToUpdate := docKeys[docIdIndexToUpdate]

				// get the old seq
				oldSeq := docInfos[docIdToUpdate].Seq

				// bump the rev of this doc
				docInfos[docIdToUpdate].Rev++
				// now udpate it
				err = db.SaveDocument(docs[docIdToUpdate], docInfos[docIdToUpdate])
				if err != nil {
					fmt.Printf("err: %v\n", err)
					return
				}
				delete(docsBySeq, oldSeq)
				docsBySeq[docInfos[docIdToUpdate].Seq] = docs[docIdToUpdate]
			} else {
				continue
			}
		} else {
			// delete doc
			if len(docKeys) > 0 {
				docIdIndexToDelete := rand.Intn(len(docKeys))
				docIdToDelete := docKeys[docIdIndexToDelete]
				// get the old seq
				oldSeq := docInfos[docIdToDelete].Seq
				err = db.SaveDocument(nil, docInfos[docIdToDelete])
				if err != nil {
					fmt.Printf("err: %v\n", err)
					return
				}
				oldInfo := docInfos[docIdToDelete]
				docKeys = append(docKeys[:docIdIndexToDelete], docKeys[docIdIndexToDelete+1:]...)
				delete(docs, docIdToDelete)
				deletedDocs[docIdToDelete] = true
				delete(docInfos, docIdToDelete)
				docCount--
				deletedCount++
				delete(docsBySeq, oldSeq)
				deletedDocsBySeq[oldInfo.Seq] = true
			} else {
				continue
			}
		}

		if i%*commitEvery == 0 {
			err = db.Commit()
			if err != nil {
				fmt.Printf("error committing %d: %v\n", i, err)
				return
			}
		}

		// final commit
		if *commitAtEnd {
			err = db.Commit()
			if err != nil {
				fmt.Printf("error committing end: %v\n", err)
				return
			}
		}

	}
}
