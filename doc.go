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
