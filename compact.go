package gouchstore

const (
	COMPACT_KEEP_ITEM int = 0
	COMPACT_DROP_ITEM int = 1
)

func defaultCompactHook(target *Gouchstore, docInfo *DocumentInfo, context interface{}) (int, error) {
	return COMPACT_KEEP_ITEM, nil
}

type compactHook func(target *Gouchstore, docInfo *DocumentInfo, context interface{}) (int, error)

type compactContext struct {
	tw          treeWriter
	targetMr    *modifyResult
	targetDb    *Gouchstore
	hook        compactHook
	hookContext interface{}
}

func (g *Gouchstore) Compact(targetFilename string) error {
	// create a compaction context
	context := compactContext{
		hook: defaultCompactHook,
	}

	// open the target database
	targetDb, err := Open(targetFilename, OPEN_CREATE)
	if err != nil {
		return err
	}
	defer targetDb.Close()

	context.targetDb = targetDb
	targetDb.header.updateSeq = g.header.updateSeq
	targetDb.header.purgeSeq = g.header.purgeSeq + 1
	targetDb.header.purgePtr = g.header.purgePtr

	if g.header.bySeqRoot != nil {
		context.tw, err = newInMemoryTreeWriter(gouchstoreIdComparator, byIdReduce, byIdReReduce, nil)
		if err != nil {
			return err
		}
		err = g.compactSeqTree(targetDb, &context)
		if err != nil {
			return err
		}
		err = context.tw.Sort()
		if err != nil {
			return err
		}
		targetDb.header.byIdRoot, err = context.tw.Write(targetDb)
		if err != nil {
			return err
		}
	}

	if g.header.localDocsRoot != nil {
		err := g.compactLocalDocsTree(targetDb, &context)
		if err != nil {
			return err
		}
	}
	if context.hook != nil {
		_, err := context.hook(g, nil, context.hookContext)
		if err != nil {
			return err
		}
	}

	err = targetDb.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (g *Gouchstore) compactLocalDocsTree(target *Gouchstore, context *compactContext) error {
	return nil
}

func (g *Gouchstore) compactSeqTree(target *Gouchstore, context *compactContext) error {

	context.targetMr = newBtreeModifyResult(gouchstoreSeqComparator, bySeqReduce, bySeqReReduce, nil, gs_DB_CHUNK_THRESHOLD, gs_DB_CHUNK_THRESHOLD)

	srcFold := lookupRequest{
		gouchstore:      g,
		compare:         gouchstoreSeqComparator,
		keys:            [][]byte{[]byte{0, 0, 0, 0, 0, 0}}, // lowest possible key
		fold:            true,
		inFold:          true,
		callbackContext: context,
		fetchCallback:   compactSeqFetchCallback,
		nodeCallback:    nil,
	}

	err := g.btreeLookup(&srcFold, g.header.bySeqRoot.pointer)
	if err != nil {
		return err
	}
	target.header.bySeqRoot, err = target.completeNewBtree(context.targetMr)
	if err != nil {
		return err
	}

	return nil

}

func compactSeqFetchCallback(req *lookupRequest, key []byte, value []byte) error {
	context := req.callbackContext.(*compactContext)

	info := &DocumentInfo{}
	decodeBySeqValue(info, value)
	if context.hook != nil {
		hookAction, err := context.hook(context.targetDb, info, context.hookContext)
		if err != nil {
			return err
		}
		if hookAction == COMPACT_DROP_ITEM {
			return nil
		}
	}

	if info.bodyPosition != 0 {
		// Copy the document from the old db file to the new one:
		data, err := req.gouchstore.readChunkAt(int64(info.bodyPosition), false)
		if err != nil {
			return err
		}

		pos, _, err := context.targetDb.writeChunk(data, false)
		if err != nil {
			return err
		}
		info.bodyPosition = uint64(pos)
		value = info.encodeBySeq()
	}

	err := outputSeqTreeItem(key, value, context)
	if err != nil {
		return err
	}

	return nil

}

func outputSeqTreeItem(k, v []byte, context *compactContext) error {
	context.targetDb.mrPushItem(k, v, context.targetMr)

	docInfo := &DocumentInfo{}
	decodeBySeqValue(docInfo, v)
	docInfo.Seq = decode_raw48(k)

	idK := []byte(docInfo.ID)
	idV := docInfo.encodeById()

	err := context.tw.AddItem(idK, idV)
	if err != nil {
		return err
	}

	return nil
}
