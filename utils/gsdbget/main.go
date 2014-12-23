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

var printBody = flag.Bool("printBody", true, "only print the document body")
var printInfo = flag.Bool("printInfo", true, "only print the document info")

func main() {

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Must specify path to a gouchstore compatible file")
		return
	} else if flag.NArg() < 2 {
		fmt.Println("Must specify document ID to get")
		return
	}
	db, err := gouchstore.Open(flag.Args()[0], gouchstore.OPEN_RDONLY)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	docInfo, err := db.DocumentInfoById(flag.Args()[1])
	if err != nil {
		fmt.Println(err)
		return
	}
	if *printInfo {
		bytes, err := json.MarshalIndent(docInfo, "", "  ")
		if err != nil {
			fmt.Println(err)
			return
		}
		if *printBody {
			fmt.Println("Document Info:")
		}
		fmt.Println(string(bytes))
	}

	doc, err := db.DocumentByDocumentInfo(docInfo)
	if err != nil {
		fmt.Println(err)
		return
	}
	if *printBody {
		if *printInfo {
			fmt.Println("Document Body:")
		}
		fmt.Println(string(doc.Body))
	}
}
