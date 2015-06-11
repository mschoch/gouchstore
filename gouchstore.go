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
	"io"
	"os"
	"sort"
)

// Document represents a document stored in the database.
type Document struct {
	ID   string
	Body []byte
}

func NewDocument(id string, value []byte) *Document {
	return &Document{
		ID:   id,
		Body: value,
	}
}

// LocalDocument represents a local (non-replicated) document.
type LocalDocument struct {
	ID      string
	Body    []byte
	Deleted bool
}

// DocumentInfo is document meta-data.
type DocumentInfo struct {
	ID           string `json:"id"`          // document identifier
	Seq          uint64 `json:"seq"`         // sequence number in database
	Rev          uint64 `json:"rev"`         // revision number of document
	RevMeta      []byte `json:"revMeta"`     // additional revision meta-data (uninterpreted by Gouchstore)
	ContentMeta  uint8  `json:"contentMeta"` // content meta-data flags
	Deleted      bool   `json:"deleted"`     // is the revision deleted?
	Size         uint64 `json:"size"`        // size of document data in bytes
	bodyPosition uint64 // byte offset of document body in file
}

func (di *DocumentInfo) WriteIDTo(w io.Writer) (int, error) {
	return w.Write([]byte(di.ID))
}

func NewDocumentInfo(id string) *DocumentInfo {
	return &DocumentInfo{
		ID:          id,
		ContentMeta: DOC_IS_COMPRESSED,
	}
}

// Compressed returns whether or not this document has been compressed for storage.
func (di *DocumentInfo) compressed() bool {
	if di.ContentMeta&DOC_IS_COMPRESSED != 0 {
		return true
	}
	return false
}

func (di *DocumentInfo) String() string {
	return fmt.Sprintf("ID: '%s' Seq: %d Rev: %d Deleted: %t Size: %d BodyPosition: %d (0x%x)", di.ID, di.Seq, di.Rev, di.Deleted, di.Size, di.bodyPosition, di.bodyPosition)
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
type DocumentInfoCallback func(gouchstore *Gouchstore, documentInfo *DocumentInfo, userContext interface{}) error

// WalkTreeCallback is a function definition which is used for tree walks.
type WalkTreeCallback func(gouchstore *Gouchstore, depth int, documentInfo *DocumentInfo, key []byte, subTreeSize uint64, reducedValue []byte, userContext interface{}) error

// Gouchstore gives access to a couchstore database file.
type Gouchstore struct {
	file   *os.File
	pos    int64
	header *header
	ops    GouchOps
}

const (
	OPEN_CREATE int = 1
	OPEN_RDONLY int = 2
)

// Open attemps to open an existing couchstore file.
//
// All Gouchstore files successfully opened should be closed with the Close() method.
func Open(filename string, options int) (*Gouchstore, error) {
	return OpenEx(filename, options, NewBaseGouchOps())
}

func OpenEx(filename string, options int, ops GouchOps) (*Gouchstore, error) {
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

	rv := Gouchstore{
		ops: ops,
	}

	file, err := rv.ops.OpenFile(filename, openFlags, 0666)
	if err != nil {
		return nil, err
	}
	rv.file = file

	rv.pos, err = rv.ops.GotoEOF(rv.file)
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

func gouchstoreFetchSingleCallback(g *Gouchstore, docInfo *DocumentInfo, userContext interface{}) error {
	resultList := userContext.(*([]*DocumentInfo))
	(*resultList)[0].ID = docInfo.ID
	(*resultList)[0].Seq = docInfo.Seq
	(*resultList)[0].Rev = docInfo.Rev
	(*resultList)[0].RevMeta = docInfo.RevMeta
	(*resultList)[0].ContentMeta = docInfo.ContentMeta
	(*resultList)[0].Size = docInfo.Size
	(*resultList)[0].Deleted = docInfo.Deleted
	(*resultList)[0].bodyPosition = docInfo.bodyPosition
	return nil
}

func gouchstoreFetchCallback(g *Gouchstore, docInfo *DocumentInfo, userContext interface{}) error {
	resultList := userContext.(*([]*DocumentInfo))
	*resultList = append(*resultList, docInfo)
	return nil
}

func (g *Gouchstore) DocumentInfoByIdNoAlloc(id string, docInfo *DocumentInfo) error {
	if g.header.byIdRoot == nil {
		return gs_ERROR_DOCUMENT_NOT_FOUND
	}

	// convert it to a slice of byte slices
	idsBytes := [][]byte{[]byte(id)}

	resultList := []*DocumentInfo{docInfo}

	lc := lookupContext{
		gouchstore:           g,
		documentInfoCallback: gouchstoreFetchSingleCallback,
		callbackContext:      &resultList,
		indexType:            gs_INDEX_TYPE_BY_ID,
	}

	lr := lookupRequest{
		compare:         gouchstoreIdComparator,
		keys:            idsBytes,
		fetchCallback:   lookupCallback,
		fold:            false,
		callbackContext: &lc,
	}

	err := g.btreeLookup(&lr, g.header.byIdRoot.pointer)
	if err != nil {
		return err
	}

	if resultList[0].ID != "" {
		return nil
	}
	return gs_ERROR_DOCUMENT_NOT_FOUND
}

// DocumentInfoById returns DocumentInfo for a single document with the specified ID.
func (g *Gouchstore) DocumentInfoById(id string) (*DocumentInfo, error) {
	docInfo := DocumentInfo{}
	err := g.DocumentInfoByIdNoAlloc(id, &docInfo)
	if err != nil {
		return nil, err
	}
	return &docInfo, nil
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

	resultList := make([]*DocumentInfo, 0)
	if g.header.byIdRoot == nil {
		return resultList, nil
	}

	lc := lookupContext{
		gouchstore:           g,
		documentInfoCallback: gouchstoreFetchCallback,
		callbackContext:      &resultList,
		indexType:            gs_INDEX_TYPE_BY_ID,
	}

	lr := lookupRequest{
		compare:         gouchstoreIdComparator,
		keys:            idsBytes,
		fetchCallback:   lookupCallback,
		fold:            false,
		callbackContext: &lc,
	}

	err := g.btreeLookup(&lr, g.header.byIdRoot.pointer)
	if err != nil {
		return nil, err
	}

	return resultList, nil
}

func lookupCallback(req *lookupRequest, key []byte, value []byte) error {
	if value == nil {
		return nil
	}

	context := req.callbackContext.(*lookupContext)

	docinfo := DocumentInfo{}
	if context.indexType == gs_INDEX_TYPE_BY_ID {
		docinfo.ID = string(key)
		decodeByIdValue(&docinfo, value)
	} else if context.indexType == gs_INDEX_TYPE_BY_SEQ {
		docinfo.Seq = decode_raw48(key)
		decodeBySeqValue(&docinfo, value)
	}

	if context.walkTreeCallback != nil {
		if context.indexType == gs_INDEX_TYPE_LOCAL_DOCS {
			// note we pass the non-initialized docinfo so we can at least detect that its a leaf
			return context.walkTreeCallback(context.gouchstore, context.depth, &docinfo, key, 0, value, context.callbackContext)
		} else {
			return context.walkTreeCallback(context.gouchstore, context.depth, &docinfo, nil, 0, nil, context.callbackContext)
		}
	} else if context.documentInfoCallback != nil {
		return context.documentInfoCallback(context.gouchstore, &docinfo, context.callbackContext)
	}

	return nil
}

func walkNodeCallback(req *lookupRequest, key []byte, value []byte) error {
	context := req.callbackContext.(*lookupContext)
	if value == nil {
		context.depth--
		return nil
	} else {
		valueNodePointer := decodeNodePointer(value)
		valueNodePointer.key = key
		err := context.walkTreeCallback(context.gouchstore, context.depth, nil, key, valueNodePointer.subtreeSize, valueNodePointer.reducedValue, context.callbackContext)
		context.depth++
		return err
	}
}

// DocumentInfoBySeq returns DocumentInfo for a single document with the specified sequence number.
func (g *Gouchstore) DocumentInfoBySeq(seq uint64) (*DocumentInfo, error) {
	docInfos, err := g.DocumentInfosBySeqs([]uint64{seq})
	if err != nil {
		return nil, err
	}
	if len(docInfos) == 1 {
		return docInfos[0], nil
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

	resultList := make([]*DocumentInfo, 0)
	if g.header.bySeqRoot == nil {
		return resultList, nil
	}

	lc := lookupContext{
		gouchstore:           g,
		documentInfoCallback: gouchstoreFetchCallback,
		callbackContext:      &resultList,
		indexType:            gs_INDEX_TYPE_BY_SEQ,
	}

	lr := lookupRequest{
		compare:         gouchstoreSeqComparator,
		keys:            idsBytes,
		fetchCallback:   lookupCallback,
		fold:            false,
		callbackContext: &lc,
	}

	err := g.btreeLookup(&lr, g.header.bySeqRoot.pointer)
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
	wtCallback := func(gouchstore *Gouchstore, depth int, documentInfo *DocumentInfo, key []byte, subTreeSize uint64, reducedValue []byte, userContext interface{}) error {
		if documentInfo != nil {
			return cb(gouchstore, documentInfo, userContext)
		}
		return nil
	}
	return g.WalkIdTree(startId, endId, wtCallback, userContext)
}

func (g *Gouchstore) WalkIdTree(startId, endId string, wtcb WalkTreeCallback, userContext interface{}) error {

	if g.header.byIdRoot == nil {
		return nil
	}

	wtcb(g, 0, nil, nil, g.header.byIdRoot.subtreeSize, g.header.byIdRoot.reducedValue, userContext)

	lc := lookupContext{
		gouchstore:       g,
		walkTreeCallback: wtcb,
		callbackContext:  userContext,
		indexType:        gs_INDEX_TYPE_BY_ID,
	}

	keys := [][]byte{[]byte(startId)}
	if endId != "" {
		keys = append(keys, []byte(endId))
	}

	lr := lookupRequest{
		compare:         gouchstoreIdComparator,
		keys:            keys,
		fetchCallback:   lookupCallback,
		nodeCallback:    walkNodeCallback,
		fold:            true,
		callbackContext: &lc,
	}

	err := g.btreeLookup(&lr, g.header.byIdRoot.pointer)
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
	wtCallback := func(gouchstore *Gouchstore, depth int, documentInfo *DocumentInfo, key []byte, subTreeSize uint64, reducedValue []byte, userContext interface{}) error {
		if documentInfo != nil {
			return cb(gouchstore, documentInfo, userContext)
		}
		return nil
	}
	return g.WalkSeqTree(since, till, wtCallback, userContext)
}

func (g *Gouchstore) WalkSeqTree(since uint64, till uint64, wtcb WalkTreeCallback, userContext interface{}) error {

	if g.header.bySeqRoot == nil {
		return nil
	}

	wtcb(g, 0, nil, nil, g.header.bySeqRoot.subtreeSize, g.header.bySeqRoot.reducedValue, userContext)

	lc := lookupContext{
		gouchstore:       g,
		walkTreeCallback: wtcb,
		callbackContext:  userContext,
		indexType:        gs_INDEX_TYPE_BY_SEQ,
	}

	keys := [][]byte{encode_raw48(since)}
	if till != 0 {
		keys = append(keys, encode_raw48(till))
	}

	lr := lookupRequest{
		compare:         gouchstoreSeqComparator,
		keys:            keys,
		fetchCallback:   lookupCallback,
		nodeCallback:    walkNodeCallback,
		fold:            true,
		callbackContext: &lc,
	}

	err := g.btreeLookup(&lr, g.header.bySeqRoot.pointer)
	if err != nil {
		return err
	}

	return nil
}

func (g *Gouchstore) DocumentBodyById(id string) ([]byte, error) {
	var doc Document
	err := g.DocumentByIdNoAlloc(id, &doc)
	if err != nil {
		return nil, err
	}
	return doc.Body, nil
}

func (g *Gouchstore) DocumentByIdNoAlloc(id string, doc *Document) error {
	var docInfo DocumentInfo
	err := g.DocumentInfoByIdNoAlloc(id, &docInfo)
	if err != nil {
		return err
	}
	err = g.DocumentByDocumentInfoNoAlloc(&docInfo, doc)
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

func (g *Gouchstore) DocumentByDocumentInfoNoAlloc(docInfo *DocumentInfo, doc *Document) error {
	var err error
	if docInfo.compressed() {
		doc.Body, err = g.readCompressedDataChunkAt(int64(docInfo.bodyPosition))
		if err != nil {
			return err
		}
	} else {
		doc.Body, err = g.readChunkAt(int64(docInfo.bodyPosition), false)
		if err != nil {
			return err
		}
	}
	doc.ID = docInfo.ID
	return nil
}

// DocumentByDocumentInfo returns the Document using the provided DocumentInfo.
// The provided DocumentInfo should be valid, such as one received by one of the
// DocumentInfo*() methods, on the current couchstore file.
func (g *Gouchstore) DocumentByDocumentInfo(docInfo *DocumentInfo) (*Document, error) {
	var rv Document
	err := g.DocumentByDocumentInfoNoAlloc(docInfo, &rv)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

// SaveDocument stores the document, if doc is nil, the document will be deleted
func (g *Gouchstore) SaveDocument(doc *Document, docInfo *DocumentInfo) error {
	return g.SaveDocuments([]*Document{doc}, []*DocumentInfo{docInfo})
}

// SaveDocuments stores multiple documents at a time
func (g *Gouchstore) SaveDocuments(docs []*Document, docInfos []*DocumentInfo) error {

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

	err := g.ops.Sync(g.file)
	if err != nil {
		return err
	}

	//Set the pos back to where it was when we started to write the real header.
	g.pos = curPos

	err = g.writeHeader(g.header)
	if err != nil {
		return err
	}

	err = g.ops.Sync(g.file)
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

func localLookupCallback(req *lookupRequest, key []byte, value []byte) error {
	localDocPointer := req.callbackContext.(*LocalDocument)
	if value == nil {
		return nil
	}

	(*localDocPointer).ID = string(key)
	(*localDocPointer).Body = value
	(*localDocPointer).Deleted = false

	return nil
}

// LocalDocumentById returns the LocalDocument with the specified identifier.
func (g *Gouchstore) LocalDocumentById(id string) (*LocalDocument, error) {
	if g.header.localDocsRoot == nil {
		return nil, gs_ERROR_DOCUMENT_NOT_FOUND
	}

	resultDocPointer := &LocalDocument{}

	lr := lookupRequest{
		compare:         gouchstoreIdComparator,
		keys:            [][]byte{[]byte(id)},
		fetchCallback:   localLookupCallback,
		fold:            false,
		callbackContext: resultDocPointer,
	}

	err := g.btreeLookup(&lr, g.header.localDocsRoot.pointer)
	if err != nil {
		return nil, err
	}

	if resultDocPointer.ID == "" {
		return nil, gs_ERROR_DOCUMENT_NOT_FOUND
	}

	return resultDocPointer, nil
}

// SaveLocalDocument stores local documents in the database
func (g *Gouchstore) SaveLocalDocument(localDoc *LocalDocument) error {
	ldUpdate := modifyAction{
		key:   []byte(localDoc.ID),
		value: localDoc.Body,
	}
	if localDoc.Deleted {
		ldUpdate.typ = gs_ACTION_REMOVE
	} else {
		ldUpdate.typ = gs_ACTION_INSERT
	}

	req := &modifyRequest{
		cmp:              gouchstoreIdComparator,
		actions:          []modifyAction{ldUpdate},
		reduce:           nil,
		rereduce:         nil,
		fetchCallback:    nil,
		compacting:       false,
		enablePurging:    false,
		purgeKP:          nil,
		purgeKV:          nil,
		kpChunkThreshold: gs_DB_CHUNK_THRESHOLD,
		kvChunkThreshold: gs_DB_CHUNK_THRESHOLD,
	}

	nroot, err := g.modifyBtree(req, g.header.localDocsRoot)
	if err != nil {
		return err
	}
	if nroot != g.header.localDocsRoot {
		g.header.localDocsRoot = nroot
	}

	return nil
}

func (g *Gouchstore) WalkLocalDocsTree(startId, endId string, wtcb WalkTreeCallback, userContext interface{}) error {

	if g.header.localDocsRoot == nil {
		return nil
	}

	wtcb(g, 0, nil, nil, g.header.localDocsRoot.subtreeSize, g.header.localDocsRoot.reducedValue, userContext)

	lc := lookupContext{
		gouchstore:       g,
		walkTreeCallback: wtcb,
		callbackContext:  userContext,
		indexType:        gs_INDEX_TYPE_LOCAL_DOCS,
	}

	keys := [][]byte{[]byte(startId)}
	if endId != "" {
		keys = append(keys, []byte(endId))
	}

	lr := lookupRequest{
		compare:         gouchstoreIdComparator,
		keys:            keys,
		fetchCallback:   lookupCallback,
		nodeCallback:    walkNodeCallback,
		fold:            true,
		callbackContext: &lc,
	}

	err := g.btreeLookup(&lr, g.header.localDocsRoot.pointer)
	if err != nil {
		return err
	}

	return nil
}

// Close will close the underlying file handle and release any resources associated with the Gouchstore object.
func (g *Gouchstore) Close() error {
	return g.ops.Close(g.file)
}

const (
	gs_DOC_IS_JSON               = 0
	gs_DOC_INVALID_JSON          = 1
	gs_DOC_INVALID_JSON_KEY      = 2
	gs_DOC_NON_JSON_MODE         = 3
	DOC_IS_COMPRESSED       byte = 128
)
