package gouchstore

import "errors"

// Interface for writing bulk data into couchstore.
// Migrated to facilitate Seriesly, re-evaluate overall API

type BulkWriter interface {
	// Set a document.
	Set(*DocumentInfo, *Document)
	// Delete a document.
	Delete(*DocumentInfo)
	// Commit the current batch.
	Commit() error
	// Shut down this bulk interface.
	Close() error
}

type instr struct {
	di  *DocumentInfo
	doc *Document
}

type bulkWriter struct {
	update chan instr
	quit   chan bool
	commit chan chan error
}

func (b *bulkWriter) Close() error {
	close(b.quit)
	return nil
}

var errClosed = errors.New("db is closed")

func (b *bulkWriter) Commit() error {
	ch := make(chan error)
	select {
	case b.commit <- ch:
		return <-ch
	case <-b.quit:
		return errClosed
	}
}

func (b *bulkWriter) Set(di *DocumentInfo, doc *Document) {
	b.update <- instr{di, doc}
}

func (b *bulkWriter) Delete(di *DocumentInfo) {
	di.Deleted = true
	b.update <- instr{di, nil}
}

func (db *Gouchstore) commitBulk(batch []instr) error {

	docs := make([]*Document, len(batch))
	docInfos := make([]*DocumentInfo, len(batch))
	for i := range batch {
		docs[i] = batch[i].doc
		docInfos[i] = batch[i].di
	}

	err := db.SaveDocuments(docs, docInfos)
	if err != nil {
		return err
	}

	return db.Commit()
}

// Get a bulk writer.
//
// You must call Close() on the bulk writer when you're done bulk
// writing.
func (db *Gouchstore) Bulk() BulkWriter {
	rv := &bulkWriter{
		make(chan instr),
		make(chan bool),
		make(chan chan error),
	}

	go func() {
		ever := true
		batch := make([]instr, 0, 100)
		for ever {
			select {
			case <-rv.quit:
				ever = false
			case req := <-rv.commit:
				req <- db.commitBulk(batch)
				batch = batch[:0]
			case i := <-rv.update:
				batch = append(batch, i)
			}
		}
	}()

	return rv
}
