# gouchstore

A native go library for working with [couchstore](https://github.com/couchbase/couchstore) files.

NOTE: currently only read-only operations are supported

## Using

    go get github.com/mschoch/gouchstore

## Example

To open a database and fetch a key:

	db, err := gouchstore.Open("database.couch")
	handleError(err)
	doc, err := db.DocumentById("docid")
	handleError(err)

## Documentation

FIXME link to godoc.org

## Utilities

There are also a few utility programs available in the utils directory.

* gsdbinfo - prints the database info for a couchstore file
* gsdblist - prints list of document info for docuemnts with ids in the specified range
* gsdbget - fetch individual documents and print the info and/or body