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
	"bytes"
	"io"
	"os"
	"strconv"
	"testing"
)

func TestCompactSmall(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	docs := make(map[string]*Document)
	deletedDocs := make(map[string]bool)
	docsBySeq := make(map[uint64]*Document)
	deletedDocsBySeq := make(map[uint64]bool)

	doc := &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":1}`),
	}
	docInfo := &DocumentInfo{
		ID:          "newdoc",
		Rev:         7,
		ContentMeta: DOC_IS_COMPRESSED,
	}

	err = db.SaveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}
	docs[doc.ID] = doc
	docsBySeq[docInfo.Seq] = doc

	oldSeq := docInfo.Seq
	doc.Body = []byte(`{"abc":2}`)
	err = db.SaveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}
	delete(docsBySeq, oldSeq)
	docsBySeq[docInfo.Seq] = doc

	oldSeq = docInfo.Seq
	doc.Body = []byte(`{"abc":3}`)
	err = db.SaveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}
	delete(docsBySeq, oldSeq)
	docsBySeq[docInfo.Seq] = doc

	err = db.Compact("compacted.couch")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove("compacted.couch")

	// open the compacted file
	compactedDb, err := Open("compacted.couch", 0)
	if err != nil {
		t.Error(err)
	}

	// verify that the state matches our expectations
	sanityCheckIdTree(t, compactedDb, docs, deletedDocs)
	sanityCheckSeqTree(t, compactedDb, docsBySeq, deletedDocsBySeq)

}

func TestCompactionLarger(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	docs := make(map[string]*Document)
	localDocs := make(map[string]*LocalDocument)
	deletedDocs := make(map[string]bool)
	docsBySeq := make(map[uint64]*Document)
	deletedDocsBySeq := make(map[uint64]bool)

	// create/update 1000 docs
	for i := 0; i < 1000; i++ {
		// add a doc, and update it 4 times
		oldSeq := uint64(0)
		for j := 0; j < 5; j++ {
			id := "doc-" + strconv.Itoa(i)
			content := "content-revision-" + strconv.Itoa(j)
			doc := &Document{
				ID:   id,
				Body: []byte(`{"abc":` + content + `}`),
			}
			docInfo := &DocumentInfo{
				ID:          id,
				Rev:         uint64(j + 1),
				ContentMeta: DOC_IS_COMPRESSED,
			}
			err := db.SaveDocument(doc, docInfo)
			if err != nil {
				t.Fatalf("error saving %d: %v", i, err)
			}
			newSeq := uint64(docInfo.Seq)
			docs[doc.ID] = doc
			docsBySeq[docInfo.Seq] = doc
			if oldSeq != 0 {
				delete(docsBySeq, oldSeq)
			}
			oldSeq = newSeq
		}
		// commit every 1000
		if i%10 == 0 {
			err := db.Commit()
			if err != nil {
				t.Fatalf("error committing %d: %v", i, err)
			}
		}
	}

	// create/update 10 local docs
	for i := 0; i < 10; i++ {
		// add a local doc, and update it 4 times
		for j := 0; j < 5; j++ {
			id := "_local/doc-" + strconv.Itoa(i)
			content := "local-content-revision-" + strconv.Itoa(j)
			doc := &LocalDocument{
				ID:   id,
				Body: []byte(`{"abc":` + content + `}`),
			}
			err := db.SaveLocalDocument(doc)
			if err != nil {
				t.Fatalf("error saving %d: %v", i, err)
			}
			localDocs[id] = doc
		}
	}

	// final commit
	err = db.Commit()
	if err != nil {
		t.Fatalf("error committing end: %v", err)
	}

	err = db.Compact("compacted.couch")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove("compacted.couch")

	// open the compacted file
	compactedDb, err := Open("compacted.couch", 0)
	if err != nil {
		t.Error(err)
	}

	// verify that the state matches our expectations
	sanityCheckIdTree(t, compactedDb, docs, deletedDocs)
	sanityCheckSeqTree(t, compactedDb, docsBySeq, deletedDocsBySeq)
	sanityCheckLocalDocsTree(t, compactedDb, localDocs)
}

func TestDiskCompactionMatchesMemory(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := OpenEx("test.couch", OPEN_CREATE, NewBaseGouchOps())
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	docs := make(map[string]*Document)
	localDocs := make(map[string]*LocalDocument)
	docsBySeq := make(map[uint64]*Document)

	// create/update 1000 docs
	for i := 0; i < 1000; i++ {
		// add a doc, and update it 4 times
		oldSeq := uint64(0)
		for j := 0; j < 5; j++ {
			id := "doc-" + strconv.Itoa(i)
			content := "content-revision-" + strconv.Itoa(j)
			doc := &Document{
				ID:   id,
				Body: []byte(`{"abc":` + content + `}`),
			}
			docInfo := &DocumentInfo{
				ID:          id,
				Rev:         uint64(j + 1),
				ContentMeta: DOC_IS_COMPRESSED,
			}
			err := db.SaveDocument(doc, docInfo)
			if err != nil {
				t.Fatalf("error saving %d: %v", i, err)
			}
			newSeq := uint64(docInfo.Seq)
			docs[doc.ID] = doc
			docsBySeq[docInfo.Seq] = doc
			if oldSeq != 0 {
				delete(docsBySeq, oldSeq)
			}
			oldSeq = newSeq
		}
		// commit every 1000
		if i%10 == 0 {
			err := db.Commit()
			if err != nil {
				t.Fatalf("error committing %d: %v", i, err)
			}
		}
	}

	// create/update 10 local docs
	for i := 0; i < 10; i++ {
		// add a local doc, and update it 4 times
		for j := 0; j < 5; j++ {
			id := "_local/doc-" + strconv.Itoa(i)
			content := "local-content-revision-" + strconv.Itoa(j)
			doc := &LocalDocument{
				ID:   id,
				Body: []byte(`{"abc":` + content + `}`),
			}
			err := db.SaveLocalDocument(doc)
			if err != nil {
				t.Fatalf("error saving %d: %v", i, err)
			}
			localDocs[id] = doc
		}
	}

	// final commit
	err = db.Commit()
	if err != nil {
		t.Fatalf("error committing end: %v", err)
	}

	err = db.Compact("compacted.couch")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove("compacted.couch")

	// close, reopen with memory compactor
	err = db.Close()
	if err != nil {
		t.Error(err)
	}
	db, err = OpenEx("test.couch", OPEN_RDONLY, NewMemCompactGouchOps())
	if err != nil {
		t.Error(err)
	}
	err = db.Compact("compacted-memory.couch")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove("compacted-memory.couch")

	compareTwoDbFiles(t, "compacted.couch", "compacted-memory.couch")
}

func compareTwoDbFiles(t *testing.T, path1, path2 string) {
	f1, err := os.Open(path1)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := os.Open(path2)
	if err != nil {
		t.Fatal(err)
	}

	offset := 0
	for err != io.EOF {

		var numRead1, numRead2 int
		buf1 := make([]byte, 16)
		numRead1, err = f1.Read(buf1)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		buf2 := make([]byte, 16)
		numRead2, err = f2.Read(buf2)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		if numRead1 != numRead2 {
			t.Fatalf("file sizes differ")
		}

		if bytes.Compare(buf1, buf2) != 0 {
			t.Fatalf("file bytes differ at offset %# x [%# x] [%# x]", offset, buf1, buf2)
		}
		offset += 16
	}

}
