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
	"fmt"
	"os"
	"sort"
)

// Document represents a document stored in the database.
type Document struct {
	ID   string
	Body []byte
}

// DocumentInfo is document meta-data.
type DocumentInfo struct {
	ID           string `json:"id"`           // document identifier
	Seq          uint64 `json:"seq"`          // sequence number in database
	Rev          uint64 `json:"rev"`          // revision number of document
	RevMeta      []byte `json:"revMeta"`      // additional revision meta-data (uninterpreted by Gouchstore)
	ContentMeta  uint8  `json:"contentMeta"`  // content meta-data flags
	Deleted      bool   `json:"deleted"`      // is the revision deleted?
	Size         uint64 `json:"size"`         // size of document data in bytes
	bodyPosition uint64 `json:"bodyPosition"` // byte offset of document body in file
}

// Compressed returns whether or not this document has been compressed for storage.
func (di *DocumentInfo) compressed() bool {
	if di.ContentMeta&gs_DOC_IS_COMPRESSED != 0 {
		return true
	}
	return false
}

func (di *DocumentInfo) String() string {
	return fmt.Sprintf("ID: `%s` Seq: %d Rev: %d Deleted: %t Size: %d BodyPosition: %d (0x%x)", di.ID, di.Seq, di.Rev, di.Deleted, di.Size, di.bodyPosition, di.bodyPosition)
}

// DatabaseInfo describes the database as a whole.
type DatabaseInfo struct {
	FileName       string `json:"fileName"`       // filesystem path
	LastSeq        uint64 `json:"lastSeq"`        // last sequence number allocated
	DocumentCount  uint64 `json:"documentCount"`  // total number of (non-deleted) documents
	DeletedCount   uint64 `json:"deletedCount"`   // total number of deleted documents
	SpaceUsed      uint64 `json:"spaceUsed"`      // disk space actively used by docs
	FileSize       uint64 `json:"fileSize"`       // total disk space used by database
	HeaderPosition uint64 `json:"headerPosition"` // file offset of current header
}

// DocumentInfoCallback is a function definition which is used for document iteration.
// For example, when iterating all documents with the AllDocuments() method,
// the provided callback will be invoked for each document in the database.
type DocumentInfoCallback func(gouchstore *Gouchstore, documentInfo *DocumentInfo, userContext interface{})

// Gouchstore gives access to a couchstore database file.
type Gouchstore struct {
	file   *os.File
	pos    int64
	header *header
}

const (
	OPEN_CREATE int = 1
	OPEN_RDONLY int = 2
)

// Open attemps to open an existing couchstore file.
//
// All Gouchstore files successfully opened should be closed with the Close() method.
func Open(filename string, options int) (*Gouchstore, error) {
	// sanity check options
	if options&OPEN_CREATE != 0 && options&OPEN_RDONLY != 0 {
		return nil, gs_ERROR_INVALID_ARGUMENTS
	}

	var openFlags int
	if options&OPEN_RDONLY != 0 {
		openFlags = os.O_RDONLY
	} else {
		openFlags = os.O_RDWR
	}

	if options&OPEN_CREATE != 0 {
		openFlags |= os.O_CREATE
	}

	file, err := os.OpenFile(filename, openFlags, 0666)
	if err != nil {
		return nil, err
	}
	rv := Gouchstore{
		file: file,
	}
	err = rv.gotoEof()
	if err != nil {
		return nil, err
	}
	if rv.pos == 0 {
		rv.header = newHeader()
		err = rv.writeHeader(rv.header)
		if err != nil {
			return nil, err
		}
	} else {
		err = rv.findLastHeader()
		if err != nil {
			return nil, err
		}
	}

	return &rv, nil
}

func gouchstoreFetchCallback(g *Gouchstore, docInfo *DocumentInfo, userContext interface{}) {
	resultList := userContext.(*([]*DocumentInfo))
	*resultList = append(*resultList, docInfo)
}

// DocumentInfoById returns DocumentInfo for a single document with the specified ID.
func (g *Gouchstore) DocumentInfoById(id string) (*DocumentInfo, error) {
	current := 0
	resultList := make([]*DocumentInfo, 0)
	err := g.btree_lookup(
		g.header.byIdRoot,
		gs_INDEX_TYPE_BY_ID,
		&current,
		[][]byte{[]byte(id)},
		gouchstoreIdComparator,
		gouchstoreFetchCallback,
		&resultList)
	if err != nil {
		return nil, err
	}
	if len(resultList) > 0 {
		return resultList[0], nil
	}
	return nil, gs_ERROR_DOCUMENT_NOT_FOUND
}

// DocumentInfosByIds returns DocumentInfo objects for the specified document IDs.
// This will be more efficient than making consecutive calls to DocumentInfoById().
//
// NOTE: contents of the result slice will be in ascending ID order, not the order they
// appeared in the argument list.
func (g *Gouchstore) DocumentInfosByIds(identifiers []string) ([]*DocumentInfo, error) {
	ids := sort.StringSlice(identifiers)
	// we need the ids in sorted order
	sort.Sort(ids)

	// convert it to a slice of byte slices
	idsBytes := make([][]byte, len(ids))
	for i, id := range ids {
		idsBytes[i] = []byte(id)
	}

	current := 0
	resultList := make([]*DocumentInfo, 0)
	err := g.btree_lookup(
		g.header.byIdRoot,
		gs_INDEX_TYPE_BY_ID,
		&current,
		idsBytes,
		gouchstoreIdComparator,
		gouchstoreFetchCallback,
		&resultList)
	if err != nil {
		return nil, err
	}
	return resultList, nil
}

// DocumentInfoBySeq returns DocumentInfo for a single document with the specified sequence number.
func (g *Gouchstore) DocumentInfoBySeq(seq uint64) (*DocumentInfo, error) {
	current := 0
	resultList := make([]*DocumentInfo, 0)
	err := g.btree_lookup(
		g.header.bySeqRoot,
		gs_INDEX_TYPE_BY_SEQ,
		&current,
		[][]byte{encode_raw48(seq)},
		gouchstoreSeqComparator,
		gouchstoreFetchCallback,
		&resultList)
	if err != nil {
		return nil, err
	}
	if len(resultList) > 0 {
		return resultList[0], nil
	}
	return nil, gs_ERROR_DOCUMENT_NOT_FOUND
}

// DocumentInfosBySeqs returns DocumentInfo objects for the specified document sequence numbers.
// This will be more efficient than making consecutive calls to DocumentInfoBySeq().
//
// NOTE: contents of the result slice will be in ascending sequence order, not the order they
// appeared in the argument list.
func (g *Gouchstore) DocumentInfosBySeqs(sequences []uint64) ([]*DocumentInfo, error) {

	seqs := seqList(sequences)
	// we need the ids in sorted order
	sort.Sort(seqs)

	// convert it to a slice of byte slices
	idsBytes := make([][]byte, len(seqs))
	for i, seq := range seqs {
		idsBytes[i] = encode_raw48(seq)
	}

	current := 0
	resultList := make([]*DocumentInfo, 0)
	err := g.btree_lookup(
		g.header.bySeqRoot,
		gs_INDEX_TYPE_BY_SEQ,
		&current,
		idsBytes,
		gouchstoreSeqComparator,
		gouchstoreFetchCallback,
		&resultList)
	if err != nil {
		return nil, err
	}
	return resultList, nil
}

// AllDocuments will iterate through all documents in the database in ascending ID order,
// from startId (inclusive) through endId (inclusive).
// For each document, the provided DocumentInfoCallback will be invoked.  A user specified context can be included,
// and this will be passed to each invocation of the callback.
//
// If startId is the empty string, the iteration will start with the first document.
//
// If endId is the empty string, the iteration will continue to the last document.
func (g *Gouchstore) AllDocuments(startId, endId string, cb DocumentInfoCallback, userContext interface{}) error {
	active := 0
	err := g.btree_range(
		g.header.byIdRoot,
		gs_INDEX_TYPE_BY_ID,
		&active,
		[]byte(startId),
		[]byte(endId),
		gouchstoreIdComparator,
		cb,
		userContext)
	if err != nil {
		return err
	}
	return nil
}

// ChangesSince will iterate through all documents in the database in ascending sequence number order,
// from since (inclusive) through till (inclusive).
// For each document, the provided DocumentInfoCallback will be invoked.  A user specified context can be included,
// and this will be passed to each invocation of the callback.
//
// If since is 0, the iteration will start with the first document.
//
// If endId is 0, the iteration will continue to the last document.
func (g *Gouchstore) ChangesSince(since uint64, till uint64, cb DocumentInfoCallback, userContext interface{}) error {
	// treat till of 0 to mean no end boundary
	var endId []byte
	if till == 0 {
		endId = []byte{}
	} else {
		endId = encode_raw48(till)
	}
	active := 0
	err := g.btree_range(
		g.header.bySeqRoot,
		gs_INDEX_TYPE_BY_SEQ,
		&active,
		encode_raw48(since),
		endId,
		gouchstoreSeqComparator,
		cb,
		userContext)
	if err != nil {
		return err
	}
	return nil
}

// DocumentById returns the Document with the specified identifier.
func (g *Gouchstore) DocumentById(id string) (*Document, error) {
	docInfo, err := g.DocumentInfoById(id)
	if err != nil {
		return nil, err
	}
	return g.DocumentByDocumentInfo(docInfo)
}

// DocumentByDocumentInfo returns the Document using the provided DocumentInfo.
// The provided DocumentInfo should be valid, such as one received by one of the
// DocumentInfo*() methods, on the current couchstore file.
func (g *Gouchstore) DocumentByDocumentInfo(docInfo *DocumentInfo) (*Document, error) {
	var rv Document
	var err error
	if docInfo.compressed() {
		rv.Body, err = g.readCompressedDataChunkAt(int64(docInfo.bodyPosition))
		if err != nil {
			return nil, err
		}
	} else {
		rv.Body, err = g.readChunkAt(int64(docInfo.bodyPosition), false)
		if err != nil {
			return nil, err
		}
	}
	rv.ID = docInfo.ID
	return &rv, nil
}

// SaveDocument not public yet while under development
func (g *Gouchstore) saveDocument(doc *Document, docInfo *DocumentInfo) error {
	return g.saveDocuments([]*Document{doc}, []*DocumentInfo{docInfo})
}

// SaveDocuments not public yet while under development
func (g *Gouchstore) saveDocuments(docs []*Document, docInfos []*DocumentInfo) error {

	numDocs := len(docs)
	seqklist := make([][]byte, numDocs)
	idklist := make([][]byte, numDocs)
	seqvlist := make([][]byte, numDocs)
	idvlist := make([][]byte, numDocs)

	seq := g.header.updateSeq

	for i, doc := range docs {
		docInfo := docInfos[i]
		seq++
		seqterm, idterm, seqval, idval, err := g.addDocToUpdateList(doc, docInfo, seq)
		if err != nil {
			return err
		}
		seqklist[i] = seqterm
		idklist[i] = idterm
		seqvlist[i] = seqval
		idvlist[i] = idval
	}

	err := g.updateIndexes(seqklist, seqvlist, idklist, idvlist)
	if err != nil {
		return err
	}

	// set the assigned sequence numbers
	seq = g.header.updateSeq
	for _, docInfo := range docInfos {
		seq++
		docInfo.Seq = seq
	}
	g.header.updateSeq = seq

	return nil
}

func (g *Gouchstore) Commit() error {
	curPos := g.pos
	var seqRootSize, idRootSize, localRootSize int64
	if g.header.bySeqRoot != nil {
		seqRootSize = gs_ROOT_BASE_SIZE + int64(len(g.header.bySeqRoot.reducedValue))
	}
	if g.header.byIdRoot != nil {
		idRootSize = gs_ROOT_BASE_SIZE + int64(len(g.header.byIdRoot.reducedValue))
	}
	if g.header.localDocsRoot != nil {
		localRootSize = gs_ROOT_BASE_SIZE + int64(len(g.header.localDocsRoot.reducedValue))
	}
	//g.pos += int64(gs_HEADER_BASE_SIZE) + seqRootSize + idRootSize + localRootSize
	//Extend file size to where end of header will land before we do first sync
	//g.writeAt([]byte{0x0}, g.pos, true)
	dummyHeader := make([]byte, int(gs_HEADER_BASE_SIZE+seqRootSize+idRootSize+localRootSize))
	g.writeChunk(dummyHeader, true)

	err := g.file.Sync()
	if err != nil {
		return err
	}

	//Set the pos back to where it was when we started to write the real header.
	g.pos = curPos

	err = g.writeHeader(g.header)
	if err != nil {
		return err
	}

	err = g.file.Sync()
	return err
}

// DatabaseInfo returns information describing the database itself.
func (g *Gouchstore) DatabaseInfo() (*DatabaseInfo, error) {
	rv := DatabaseInfo{
		FileName:       g.file.Name(),
		LastSeq:        g.header.updateSeq,
		FileSize:       uint64(g.pos),
		HeaderPosition: g.header.position,
	}
	if g.header.byIdRoot != nil {
		rv.DocumentCount = decode_raw40(g.header.byIdRoot.reducedValue[0:5])
		rv.DeletedCount = decode_raw40(g.header.byIdRoot.reducedValue[5:10])
		rv.SpaceUsed = decode_raw48(g.header.byIdRoot.reducedValue[10:16])
		rv.SpaceUsed += g.header.byIdRoot.subtreeSize
	}
	if g.header.bySeqRoot != nil {
		rv.SpaceUsed += g.header.bySeqRoot.subtreeSize
	}
	if g.header.localDocsRoot != nil {
		rv.SpaceUsed += g.header.localDocsRoot.subtreeSize
	}
	return &rv, nil
}

// Close will close the underlying file handle and release any resources associated with the Gouchstore object.
func (g *Gouchstore) Close() error {
	return g.file.Close()
}

const (
	gs_DOC_IS_JSON               = 0
	gs_DOC_INVALID_JSON          = 1
	gs_DOC_INVALID_JSON_KEY      = 2
	gs_DOC_NON_JSON_MODE         = 3
	gs_DOC_IS_COMPRESSED    byte = 128
)
