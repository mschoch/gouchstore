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
	"os"
	"reflect"
	"testing"
)

const testFileName = "test/couchbase_beer_sample_vbucket.couch"

var testFile, _ = os.Open(testFileName)
var testFileInfo, _ = testFile.Stat()

func TestGouchstoreDatabaseInfo(t *testing.T) {

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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

	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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
	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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
	db, err := Open("test/couchbase_beer_sample_vbucket.couch")
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
