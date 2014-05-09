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
	"encoding/json"
	"flag"
	"fmt"

	"github.com/mschoch/gouchstore"
)

var startId = flag.String("startId", "", "the document ID to scan from")
var endId = flag.String("endId", "", "the document ID to scan to")
var startSeq = flag.Int("startSeq", -1, "the sequence number to scan from")
var endSeq = flag.Int("endSeq", -1, "the sequence number to scan to")

func allDocumentsCallback(g *gouchstore.Gouchstore, docInfo *gouchstore.DocumentInfo, userContext interface{}) error {
	bytes, err := json.MarshalIndent(docInfo, "", "  ")
	if err != nil {
		fmt.Println(err)
	} else {
		userContext.(map[string]int)["count"]++
		fmt.Println(string(bytes))
	}
	return nil
}

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

	context := map[string]int{"count": 0}

	// sequence mode
	if *startSeq != -1 || *endSeq != -1 {
		if *startSeq < 0 {
			*startSeq = 0
		}
		db.ChangesSince(uint64(*startSeq), uint64(*endSeq), allDocumentsCallback, context)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else { // id mode
		err = db.AllDocuments(*startId, *endId, allDocumentsCallback, context)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	fmt.Printf("Listed %d documents\n", context["count"])
}
