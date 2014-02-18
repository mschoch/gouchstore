//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
/*
Package gouchstore implements native Go access to couchstore files.  See https://github.com/couchbase/couchstore for more information about couchstore.

Usage:

	db, err := gouchstore.Open("database.couch")

To fetch a document by ID:

	doc, err := db.DocumentById("docid")

To list all keys, create a callback:

	func callback(g *gouchstore.Gouchstore, docInfo *gouchstore.DocumentInfo, userContext interface{}) {
		fmt.Printf("ID: %s", docInfo.ID)
	}

Then:

	err = db.AllDocuments("", "", callback, nil)


*/
package gouchstore
