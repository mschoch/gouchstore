//  Copyright (c) 2014 Marty Schoch
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package gouchstore

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
)

const testFileName = "test/couchbase_beer_sample_vbucket.couch"

var testFile, _ = os.Open(testFileName)
var testFileInfo, _ = testFile.Stat()

func TestGouchstoreDatabaseInfo(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	dbInfo, err := db.DatabaseInfo()
	if err != nil {
		t.Fatal(err)
	}

	expectedDbInfo := &DatabaseInfo{
		FileName:       "test/couchbase_beer_sample_vbucket.couch",
		LastSeq:        0x65,
		DocumentCount:  0x65,
		DeletedCount:   0,
		FileSize:       uint64(testFileInfo.Size()),
		HeaderPosition: 0x39000,
		SpaceUsed:      0xaaed,
	}

	if !reflect.DeepEqual(dbInfo, expectedDbInfo) {
		t.Errorf("expected %#v, got %#v", expectedDbInfo, dbInfo)
	}
}

func TestGouchstoreDocumentInfoById(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	docInfo, err := db.DocumentInfoById("rogue_ales-hazelnut_brown_nectar")
	if err != nil {
		t.Error(err)
	}

	expectedDocInfo := &DocumentInfo{
		ID:           "rogue_ales-hazelnut_brown_nectar",
		Seq:          0x4d,
		Rev:          0x1,
		RevMeta:      []byte{0x0, 0x0, 0x4, 0xf, 0xb5, 0x69, 0xa1, 0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		ContentMeta:  0x80,
		Deleted:      false,
		Size:         0x35a,
		bodyPosition: 0x64a6,
	}

	if !reflect.DeepEqual(docInfo, expectedDocInfo) {
		t.Errorf("expected %#v, got %#v", expectedDocInfo, docInfo)
	}
}

func TestGouchstoreDocumentInfosByIds(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	idsToFind := []string{
		"triple_rock_brewery-amber",
		"rogue_ales-hazelnut_brown_nectar",
	}
	docInfos, err := db.DocumentInfosByIds(idsToFind)
	if err != nil {
		t.Error(err)
	}

	expectedDocInfos := []*DocumentInfo{
		&DocumentInfo{
			ID:           "rogue_ales-hazelnut_brown_nectar",
			Seq:          0x4d,
			Rev:          0x1,
			RevMeta:      []byte{0x0, 0x0, 0x4, 0xf, 0xb5, 0x69, 0xa1, 0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			ContentMeta:  0x80,
			Deleted:      false,
			Size:         0x35a,
			bodyPosition: 0x64a6,
		},
		&DocumentInfo{
			ID:           "triple_rock_brewery-amber",
			Seq:          0x5e,
			Rev:          0x1,
			RevMeta:      []byte{0x0, 0x0, 0x4, 0x10, 0x7d, 0x8c, 0x98, 0x1a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			ContentMeta:  0x80,
			Deleted:      false,
			Size:         0xda,
			bodyPosition: 0x818b,
		},
	}

	if !reflect.DeepEqual(docInfos, expectedDocInfos) {
		t.Errorf("expected %#v, got %#v", expectedDocInfos, docInfos)
	}

}

func TestGouchstoreDocumentInfoBySeq(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	docInfo, err := db.DocumentInfoBySeq(0x4d)
	if err != nil {
		t.Error(err)
	}

	expectedDocInfo := &DocumentInfo{
		ID:           "rogue_ales-hazelnut_brown_nectar",
		Seq:          0x4d,
		Rev:          0x1,
		RevMeta:      []byte{0x0, 0x0, 0x4, 0xf, 0xb5, 0x69, 0xa1, 0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		ContentMeta:  0x80,
		Deleted:      false,
		Size:         0x35a,
		bodyPosition: 0x64a6,
	}

	if !reflect.DeepEqual(docInfo, expectedDocInfo) {
		t.Errorf("expected %#v, got %#v", expectedDocInfo, docInfo)
	}

}

func TestGouchstoreDocumentInfosBySeqs(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	seqsToFind := []uint64{
		0x4d,
		0x32,
	}
	docInfos, err := db.DocumentInfosBySeqs(seqsToFind)
	if err != nil {
		t.Error(err)
	}

	expectedDocInfos := []*DocumentInfo{
		&DocumentInfo{
			ID:           "lion_brewery_ceylon_ltd",
			Seq:          0x32,
			Rev:          0x1,
			RevMeta:      []byte{0x0, 0x0, 0x4, 0xe, 0xbe, 0x60, 0x57, 0xf2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			ContentMeta:  0x80,
			Deleted:      false,
			Size:         0x136,
			bodyPosition: 0x3e5a,
		},
		&DocumentInfo{
			ID:           "rogue_ales-hazelnut_brown_nectar",
			Seq:          0x4d,
			Rev:          0x1,
			RevMeta:      []byte{0x0, 0x0, 0x4, 0xf, 0xb5, 0x69, 0xa1, 0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			ContentMeta:  0x80,
			Deleted:      false,
			Size:         0x35a,
			bodyPosition: 0x64a6,
		},
	}

	if !reflect.DeepEqual(docInfos, expectedDocInfos) {
		t.Errorf("expected %#v, got %#v", expectedDocInfos, docInfos)
	}

}

type testAllDocumentsContext struct {
	t     *testing.T
	count int
}

func testAllDocumentsCallback(g *Gouchstore, docInfo *DocumentInfo, userContext interface{}) {
	context := userContext.(*testAllDocumentsContext)
	context.count++
}

func TestGouchstoreAllDocuments(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	context := testAllDocumentsContext{
		t:     t,
		count: 0,
	}
	err = db.AllDocuments("", "", testAllDocumentsCallback, &context)
	if err != nil {
		t.Error(err)
	}
	if context.count != 101 {
		t.Errorf("expected count %d, got %d", 101, context.count)
	}

	// now scan a range
	context.count = 0
	err = db.AllDocuments("c", "d", testAllDocumentsCallback, &context)
	if err != nil {
		t.Error(err)
	}
	if context.count != 4 {
		t.Errorf("expected count %d, got %d", 4, context.count)
	}

}

func TestGouchstoreChangesSince(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	context := testAllDocumentsContext{
		t:     t,
		count: 0,
	}
	err = db.ChangesSince(0, 0, testAllDocumentsCallback, &context)
	if err != nil {
		t.Error(err)
	}
	if context.count != 101 {
		t.Errorf("expected count %d, got %d", 101, context.count)
	}

	// now scan a range
	context.count = 0
	err = db.ChangesSince(10, 19, testAllDocumentsCallback, &context)
	if err != nil {
		t.Error(err)
	}
	if context.count != 10 {
		t.Errorf("expected count %d, got %d", 10, context.count)
	}

}

func TestGouchstoreDocumentById(t *testing.T) {
	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	doc, err := db.DocumentById("rogue_ales-hazelnut_brown_nectar")
	if err != nil {
		t.Error(err)
	}
	if string(doc.Body) != `{"name":"Hazelnut Brown Nectar","abv":6.0,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"rogue_ales","updated":"2010-07-22 20:00:20","description":"HazelNut Brown Nectar is a nutty twist to a traditional European Brown Ale. Dark brown in color with a hazelnut aroma, a rich nutty flavor and a smooth malty finish. Dedicated to the homebrewer in each of us--the homebrewer who inspired this creation is Chris Studach, a friend of Rogues resident wizard John Maier, who added a Northwest twist to the classic style by adding hazelnuts for the host homebrew at the 1993 American Homebrewers Association convention. Chris put the nut in nut brown!\r\n\r\nHazelnut Brown Nectar Ale is a blend of Great Western 2-row Pale, Munich, Hugh Baird Brown, Crystal 80 and Crystal 135, Carastan, and Beeston Pale Chocolate malts; hazelnut extract; Perle and Saaz hops. HazelNut Brown Nectar is available in a 22-ounce bottle, a special commemorative 3-litre bottle with ceramic swing-top, and on draft.","style":"American-Style Brown Ale","category":"North American Ale"}` {
		t.Errorf("invalid document body")
	}
}

func TestGouchstoreDocumentByDocumentInfo(t *testing.T) {
	db, err := Open("test/couchbase_beer_sample_vbucket.couch", OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	docInfo, err := db.DocumentInfoById("rogue_ales-hazelnut_brown_nectar")
	if err != nil {
		t.Error(err)
	}

	doc, err := db.DocumentByDocumentInfo(docInfo)
	if err != nil {
		t.Error(err)
	}
	if string(doc.Body) != `{"name":"Hazelnut Brown Nectar","abv":6.0,"ibu":0.0,"srm":0.0,"upc":0,"type":"beer","brewery_id":"rogue_ales","updated":"2010-07-22 20:00:20","description":"HazelNut Brown Nectar is a nutty twist to a traditional European Brown Ale. Dark brown in color with a hazelnut aroma, a rich nutty flavor and a smooth malty finish. Dedicated to the homebrewer in each of us--the homebrewer who inspired this creation is Chris Studach, a friend of Rogues resident wizard John Maier, who added a Northwest twist to the classic style by adding hazelnuts for the host homebrew at the 1993 American Homebrewers Association convention. Chris put the nut in nut brown!\r\n\r\nHazelnut Brown Nectar Ale is a blend of Great Western 2-row Pale, Munich, Hugh Baird Brown, Crystal 80 and Crystal 135, Carastan, and Beeston Pale Chocolate malts; hazelnut extract; Perle and Saaz hops. HazelNut Brown Nectar is available in a 22-ounce bottle, a special commemorative 3-litre bottle with ceramic swing-top, and on draft.","style":"American-Style Brown Ale","category":"North American Ale"}` {
		t.Errorf("invalid document body")
	}
}

func TestOpenInvalidArguments(t *testing.T) {
	_, err := Open("", OPEN_CREATE|OPEN_RDONLY)
	if err != gs_ERROR_INVALID_ARGUMENTS {
		t.Errorf("expected invalid arguments, got %v", err)
	}
}

func TestOpenNonexistantWithoutCreateOption(t *testing.T) {
	_, err := Open("/doesnotexist", 0)
	if err == nil {
		t.Errorf("expected error opening non-existant file without create option, got nil")
	}
}

func TestCreateNew(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	if db.pos != gs_BLOCK_MARKER_SIZE+gs_CHUNK_LENGTH_SIZE+gs_CHUNK_CRC_SIZE+gs_HEADER_BASE_SIZE {
		t.Errorf("expected new db pos to be: %d, got %d", gs_BLOCK_MARKER_SIZE+gs_CHUNK_LENGTH_SIZE+gs_CHUNK_CRC_SIZE+gs_HEADER_BASE_SIZE, db.pos)
	}
}

func TestAddDocumentToEmpty(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	doc := &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":123}`),
	}
	docInfo := &DocumentInfo{
		ID:          "newdoc",
		Rev:         7,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	if db.header.updateSeq != 1 {
		t.Errorf("expected update seq to be 1, got %d", db.header.updateSeq)
	}

	if db.header.byIdRoot == nil {
		t.Errorf("expected by id root to no longer be nil")
	}
	if db.header.bySeqRoot == nil {
		t.Errorf("expected by seq root to no longer be nil")
	}

	// check document exists
	assertDocsExistWithContent(t, db, []*Document{doc}, []*DocumentInfo{docInfo})

	// test that another non-existant docs dont exist
	_, err = db.DocumentInfoById("does-not-exist")
	if err != gs_ERROR_DOCUMENT_NOT_FOUND {
		t.Errorf("expected document not found for key `does-not-exist`")
	}

	// test that another non-existant docs dont exist
	_, err = db.DocumentInfoBySeq(255)
	if err != gs_ERROR_DOCUMENT_NOT_FOUND {
		t.Errorf("expected document not found for seq 255")
	}
}

func TestAddMultipleDocumentsToEmpty(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	doc := &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":123}`),
	}
	docInfo := &DocumentInfo{
		ID:          "newdoc",
		Rev:         7,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	if db.header.updateSeq != 1 {
		t.Errorf("expected update seq to be 1, got %d", db.header.updateSeq)
	}

	if db.header.byIdRoot == nil {
		t.Errorf("expected by id root to no longer be nil")
	}
	if db.header.bySeqRoot == nil {
		t.Errorf("expected by seq root to no longer be nil")
	}

	// check document exists
	assertDocsExistWithContent(t, db, []*Document{doc}, []*DocumentInfo{docInfo})

	// add another document
	doc = &Document{
		ID:   "newdoc2",
		Body: []byte(`{"abc":456}`),
	}
	docInfo = &DocumentInfo{
		ID:          "newdoc2",
		Rev:         9,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	if db.header.updateSeq != 2 {
		t.Errorf("expected update seq to be 2, got %d", db.header.updateSeq)
	}

	// check document exists
	assertDocsExistWithContent(t, db, []*Document{doc}, []*DocumentInfo{docInfo})
}

func TestUpdateDocument(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	doc := &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":123}`),
	}
	docInfo := &DocumentInfo{
		ID:          "newdoc",
		Rev:         7,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	if db.header.updateSeq != 1 {
		t.Errorf("expected update seq to be 1, got %d", db.header.updateSeq)
	}

	if db.header.byIdRoot == nil {
		t.Errorf("expected by id root to no longer be nil")
	}
	if db.header.bySeqRoot == nil {
		t.Errorf("expected by seq root to no longer be nil")
	}

	// check document exists
	assertDocsExistWithContent(t, db, []*Document{doc}, []*DocumentInfo{docInfo})

	// update the document
	doc = &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":456}`),
	}
	docInfo = &DocumentInfo{
		ID:          "newdoc",
		Rev:         9,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	if db.header.updateSeq != 2 {
		t.Errorf("expected update seq to be 2, got %d", db.header.updateSeq)
	}

	// check correct docuemnts exist
	assertDocsExistWithContent(t, db, []*Document{doc}, []*DocumentInfo{docInfo})
}

func TestDeleteDocument(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	doc := &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":123}`),
	}
	docInfo := &DocumentInfo{
		ID:          "newdoc",
		Rev:         7,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	// add a document
	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	// delete document
	doc = nil
	docInfo = &DocumentInfo{
		ID:          "newdoc",
		Rev:         9,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.saveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	if db.header.updateSeq != 2 {
		t.Errorf("expected update seq to be 2, got %d", db.header.updateSeq)
	}

	// try and retrieve the document we just added
	di, err := db.DocumentInfoById("newdoc")
	if err != nil {
		t.Errorf("expected to find doc info for document just added, got: %v", err)
	}

	if di.Rev != docInfo.Rev {
		t.Errorf("expected doc info revision to match what we passed in")
	}

	if !di.Deleted {
		t.Errorf("expected to info deleted to be true")
	}

	if di.bodyPosition != 0 {
		t.Errorf("expected body pos of deleted doc to be 0")
	}
}

func assertDocsExistWithContent(t *testing.T, db *Gouchstore, docs []*Document, docInfos []*DocumentInfo) {
	for i, di := range docInfos {
		dbdi, err := db.DocumentInfoById(di.ID)
		if err != nil {
			t.Error(err)
		}
		if dbdi.Rev != di.Rev {
			t.Errorf("expected database doc info rev %d to match doc info rev %d", dbdi.Rev, di.Rev)
		}

		d := docs[i]
		// now try to get the document content
		dbd, err := db.DocumentByDocumentInfo(dbdi)
		if err != nil {
			t.Errorf("expected to find document body just added, got: %v", err)
		}

		if !reflect.DeepEqual(d, dbd) {
			t.Errorf("expected doc retrieved: %#v to match document added: %#v", dbd, d)
		}
	}
}

func TestComittedChangesPersist(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	docInfos := []*DocumentInfo{
		&DocumentInfo{
			ID:          "a",
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		},
		&DocumentInfo{
			ID:          "b",
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		},
		&DocumentInfo{
			ID:          "e",
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		},
		&DocumentInfo{
			ID:          "c",
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		},
		&DocumentInfo{
			ID:          "d",
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		},
	}
	docs := []*Document{
		&Document{
			ID:   "a",
			Body: []byte("aaaaaaaaaa"),
		},
		&Document{
			ID:   "b",
			Body: []byte("bbbbbbbbbb"),
		},
		&Document{
			ID:   "e",
			Body: []byte("eeeeeeeeee"),
		},
		&Document{
			ID:   "c",
			Body: []byte("cccccccccc"),
		},
		&Document{
			ID:   "d",
			Body: []byte("dddddddddd"),
		},
	}

	err = db.saveDocuments(docs, docInfos)
	if err != nil {
		t.Error(err)
	}

	// commit the changes
	err = db.Commit()
	if err != nil {
		t.Error(err)
	}

	// close the file
	err = db.Close()

	// now open it up again
	db, err = Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}

	// check the results
	assertDocsExistWithContent(t, db, docs, docInfos)
}

func TestCreateLargerFile(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	for i := 0; i < 10000; i++ {
		id := "doc-" + strconv.Itoa(i)
		doc := &Document{
			ID:   id,
			Body: []byte(`{"abc":123}`),
		}
		docInfo := &DocumentInfo{
			ID:          id,
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		}
		err := db.saveDocument(doc, docInfo)
		if err != nil {
			t.Fatalf("error saving %d: %v", i, err)
		}
		// commit every 1000
		if i%1000 == 0 {
			err := db.Commit()
			if err != nil {
				t.Fatalf("error committing %d: %v", i, err)
			}
		}
	}
	// final commit
	err = db.Commit()
	if err != nil {
		t.Fatalf("error committing end: %v", err)
	}

	// check the tree?
	sanityCheckIdTree(t, db, 10000, 0)
}

func TestCreateLargerFileAndUpdateThemAll(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		id := "doc-" + strconv.Itoa(i)
		doc := &Document{
			ID:   id,
			Body: []byte(`{"abc":123}`),
		}
		docInfo := &DocumentInfo{
			ID:          id,
			Rev:         1,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		}
		err := db.saveDocument(doc, docInfo)
		if err != nil {
			t.Fatalf("error saving %d: %v", i, err)
		}
		// commit every 1000
		if i%1000 == 0 {
			err := db.Commit()
			if err != nil {
				t.Fatalf("error committing %d: %v", i, err)
			}
		}
	}
	// final commit
	err = db.Commit()
	if err != nil {
		t.Fatalf("error committing end: %v", err)
	}

	// check the tree
	sanityCheckIdTree(t, db, 100, 0)

	// close
	db.Close()

	// reopen
	db, err = Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}

	// add the same docs again (update them)
	for i := 0; i < 100; i++ {
		id := "doc-" + strconv.Itoa(i)
		doc := &Document{
			ID:   id,
			Body: []byte(`{"abc":123}`),
		}
		docInfo := &DocumentInfo{
			ID:          id,
			Rev:         2,
			ContentMeta: gs_DOC_IS_COMPRESSED,
		}
		err := db.saveDocument(doc, docInfo)
		if err != nil {
			t.Fatalf("error saving %d: %v", i, err)
		}

		// commit every operation this pass
		err = db.Commit()
		if err != nil {
			t.Fatalf("error committing %d: %v", i, err)
		}

		// check that we still have 100 docs
		sanityCheckIdTree(t, db, 100, 0)

	}
	// final commit
	err = db.Commit()
	if err != nil {
		t.Fatalf("error committing end: %v", err)
	}
}

type sanityCheckIdTreeContext struct {
	totalSize    uint64
	docCount     uint64
	deletedCount uint64
}

func sanityCheckIdTree(t *testing.T, db *Gouchstore, docCount, deletedCount uint64) {
	wtCallback := func(gouchstore *Gouchstore, depth int, documentInfo *DocumentInfo, key []byte, subTreeSize uint64, reducedValue []byte, userContext interface{}) {

		context := userContext.(*sanityCheckIdTreeContext)

		if documentInfo != nil {
			context.totalSize += documentInfo.Size
			if documentInfo.Deleted {
				context.deletedCount++
			} else {
				context.docCount++
			}
		}
	}

	context := new(sanityCheckIdTreeContext)
	db.WalkIdTree("", "", wtCallback, context)
	rdocCount, rdeletedCount, rtotalSize := decodeByIdReduce(db.header.byIdRoot.reducedValue)
	if context.docCount != rdocCount {
		t.Errorf("Expected reduced document count %d to match document count %d", rdocCount, context.docCount)
	}
	if context.deletedCount != rdeletedCount {
		t.Errorf("Expected reduced deleted document count %d to match deleted documnt count %d", rdocCount, context.docCount)
	}
	if context.totalSize != rtotalSize {
		t.Errorf("Expected reduced total size %d to match total size %d", rtotalSize, context.totalSize)
	}
	if rdocCount != docCount {
		t.Errorf("Expected document to be %d got %d", docCount, rdocCount)
	}
	if rdeletedCount != deletedCount {
		t.Errorf("Expected document to be %d got %d", deletedCount, rdeletedCount)
	}
}

func TestRealWorld(t *testing.T) {
	// fix the seed for this test so its repeatable
	rand.Seed(25)
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	var docCount uint64 = 0
	var deletedCount uint64 = 0
	nextDocId := 0
	docs := make(map[string]*Document)
	docInfos := make(map[string]*DocumentInfo)
	docKeys := make([]string, 0)
	// iterate through this many operations
	for i := 0; i < 10000; i++ {
		operation := rand.Intn(10)
		if operation < 7 {
			// add doc
			docId := "doc-" + strconv.Itoa(nextDocId)
			doc := &Document{
				ID:   docId,
				Body: []byte(`{"content":123}`),
			}
			nextDocId++
			docInfo := &DocumentInfo{
				ID:          docId,
				Rev:         1,
				ContentMeta: gs_DOC_IS_COMPRESSED,
			}
			err = db.saveDocument(doc, docInfo)
			if err != nil {
				t.Error(err)
			}
			docs[docId] = doc
			docInfos[docId] = docInfo
			docKeys = append(docKeys, docId)
			docCount++
		} else if operation < 9 {
			// update doc
			docIdIndexToUpdate := rand.Intn(len(docKeys))
			docIdToUpdate := docKeys[docIdIndexToUpdate]
			// bump the rev of this doc
			docInfos[docIdToUpdate].Rev++
			// now udpate it
			err = db.saveDocument(docs[docIdToUpdate], docInfos[docIdToUpdate])
			if err != nil {
				t.Error(err)
			}
		} else {
			// delete doc
			if len(docKeys) > 0 {
				docIdIndexToDelete := rand.Intn(len(docKeys))
				docIdToDelete := docKeys[docIdIndexToDelete]
				err = db.saveDocument(nil, docInfos[docIdToDelete])
				if err != nil {
					t.Error(err)
				}
				docKeys = append(docKeys[:docIdIndexToDelete], docKeys[docIdIndexToDelete+1:]...)
				delete(docs, docIdToDelete)
				delete(docInfos, docIdToDelete)
				docCount--
				deletedCount++
			}
		}

		// commit every 10 operations
		if i%10 == 0 {
			err = db.Commit()
			if err != nil {
				t.Errorf("error committing %d: %v", i, err)
			}
		}

		// close and reopen every 100 operations
		if i%100 == 0 {
			err = db.Close()
			if err != nil {
				t.Error(err)
			}
			db, err = Open("test.couch", 0)
			if err != nil {
				t.Error(err)
			}
		}

		// verify that the state matches our expectations
		sanityCheckIdTree(t, db, docCount, deletedCount)
	}
}
