package gouchstore

import (
	"os"
	"strconv"
	"testing"
)

func TestCompact(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	docs := make(map[string]*Document)
	deletedDocs := make(map[string]bool)

	doc := &Document{
		ID:   "newdoc",
		Body: []byte(`{"abc":1}`),
	}
	docInfo := &DocumentInfo{
		ID:          "newdoc",
		Rev:         7,
		ContentMeta: gs_DOC_IS_COMPRESSED,
	}

	err = db.SaveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}
	docs[doc.ID] = doc

	doc.Body = []byte(`{"abc":2}`)
	err = db.SaveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
	}

	doc.Body = []byte(`{"abc":3}`)
	err = db.SaveDocument(doc, docInfo)
	if err != nil {
		t.Error(err)
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
	sanityCheckSeqTree(t, compactedDb, 1, 0)

}

func TestCompactionLarger(t *testing.T) {
	defer os.Remove("test.couch")
	db, err := Open("test.couch", OPEN_CREATE)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	docs := make(map[string]*Document)
	deletedDocs := make(map[string]bool)

	// create/update 1000 docs
	for i := 0; i < 1000; i++ {
		// add a doc, and update it 4 times
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
				ContentMeta: gs_DOC_IS_COMPRESSED,
			}
			err := db.SaveDocument(doc, docInfo)
			if err != nil {
				t.Fatalf("error saving %d: %v", i, err)
			}
			docs[doc.ID] = doc
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
	sanityCheckSeqTree(t, compactedDb, 1000, 0)
}
