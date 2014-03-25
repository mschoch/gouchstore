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

	"github.com/mschoch/gouchstore"
)

var memCompact = flag.Bool("memCompact", false, "perform compaction in memory")

func main() {

	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Must specify path to a gouchstore compatible file")
		return
	} else if flag.NArg() < 2 {
		fmt.Println("Must specify path to the new compacted gouchstore file")
		return
	}

	var ops gouchstore.GouchOps
	if *memCompact {
		ops = gouchstore.NewMemCompactGouchOps()
	} else {
		ops = gouchstore.NewBaseGouchOps()
	}
	db, err := gouchstore.OpenEx(flag.Args()[0], gouchstore.OPEN_RDONLY, ops)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	err = db.Compact(flag.Arg(1))
	if err != nil {
		fmt.Println(err)
	}
}
