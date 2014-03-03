package gouchstore

type DocumentWalkFun func(db *Gouchstore, di *DocumentInfo, doc *Document) error

// Walk the DB from a specific location including the complete docs.
func (db *Gouchstore) WalkDocs(startkey, endkey string, callback DocumentWalkFun) error {

	return db.AllDocuments(startkey, endkey, func(fdb *Gouchstore, di *DocumentInfo, context interface{}) error {
		doc, err := fdb.DocumentByDocumentInfo(di)
		if err != nil {
			return err
		}
		return callback(fdb, di, doc)
	}, nil)

}
