# gouchstore

A native go library for working with [couchstore](https://github.com/couchbase/couchstore) files.

NOTE: This library is relatively new, use at your own risk.

## Using

    go get github.com/mschoch/gouchstore

## Example

To open a database and fetch a key:

	db, err := gouchstore.Open("database.couch")
	handleError(err)
	doc, err := db.DocumentById("docid")
	handleError(err)

## Documentation

See the [full documentation](http://godoc.org/github.com/mschoch/gouchstore)

## Utilities

There are also a few utility programs available in the utils directory.

* gsdbinfo - prints the database info for a couchstore file

		$ gsdbinfo test/couchbase_beer_sample_vbucket.couch 
		{
		  "fileName": "/Users/mschoch/go/src/github.com/mschoch/gouchstore/test/couchbase_beer_sample_vbucket.couch",
		  "lastSeq": 101,
		  "documentCount": 101,
		  "deletedCount": 0,
		  "spaceUsed": 43757,
		  "FileSize": 233563,
		  "HeaderPosition": 233472
		}

* gsdblist - prints list of document info for documents with ids in the specified id range or the specified sequence range

		$ gsdblist -startId ab -endId ac test/couchbase_beer_sample_vbucket.couch
		{
		  "id": "abita_brewing_company-s_o_s",
		  "seq": 1,
		  "rev": 1,
		  "revMeta": "AAAEDEOgB8AAAAAAAAAAAA==",
		  "contentMeta": 128,
		  "deleted": false,
		  "size": 314
		}
		Listed 1 documents
		$ gsdblist -startSeq 101 -endSeq 101 test/couchbase_beer_sample_vbucket.couch 
		{
		  "id": "zea_rotisserie_and_brewery-clearview_light",
		  "seq": 101,
		  "rev": 1,
		  "revMeta": "AAAEENA2njwAAAAAAAAAAA==",
		  "contentMeta": 128,
		  "deleted": false,
		  "size": 167
		}
		Listed 1 documents

* gsdbget - fetch individual documents and print the info and/or body

		$ gsdbget test/couchbase_beer_sample_vbucket.couch lion_brewery_ceylon_ltd
		Document Info:
		{
		  "id": "lion_brewery_ceylon_ltd",
		  "seq": 50,
		  "rev": 1,
		  "revMeta": "AAAEDr5gV/IAAAAAAAAAAA==",
		  "contentMeta": 128,
		  "deleted": false,
		  "size": 310
		}
		Document Body:
		{"name":"Lion Brewery Ceylon Ltd.","city":"Colombo","state":"","code":"","country":"Sri Lanka","phone":"94-331535-42","website":"http://www.lionbeer.com/","type":"brewery","updated":"2010-07-22 20:00:20","description":"","address":["No-254, Colombo Road"],"geo":{"accuracy":"APPROXIMATE","lat":38.7548,"lon":-9.1883}}

* gsdbcompact - compact a couchstore file

		$  gsdbcompact original.couch compacted.couch

## Build Status

[![Build Status](https://drone.io/github.com/mschoch/gouchstore/status.png)](https://drone.io/github.com/mschoch/gouchstore/latest)

## Code Coverage

[![Coverage Status](https://coveralls.io/repos/mschoch/gouchstore/badge.png?branch=master)](https://coveralls.io/r/mschoch/gouchstore?branch=master)