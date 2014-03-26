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
	"fmt"
	"sort"
)

const gs_DB_CHUNK_THRESHOLD int = 1279

type btreeFetchCallback func(req *modifyRequest, k []byte, v []byte, context interface{})
type btreePurgeKPFunc func(np *nodePointer, context interface{}) int
type btreePurgeKVFunc func(key, val []byte, context interface{}) int

type nodeList struct {
	data    []byte
	key     []byte
	pointer *nodePointer
	next    *nodeList
}

type indexUpdateContext struct {
	seqacts []modifyAction
	actpos  int
	seqs    [][]byte
	seqvals [][]byte
	valpos  int
}

const (
	gs_ACTION_FETCH  int = 0
	gs_ACTION_REMOVE int = 1
	gs_ACTION_INSERT int = 2
)

const (
	gs_PURGE_ITEM    int = 0
	gs_PURGE_STOP    int = 1
	gs_PURGE_KEEP    int = 2
	gs_PURGE_PARTIAL int = 3
)

const (
	gs_KP_NODE int = 0
	gs_KV_NODE int = 1
)

type modifyAction struct {
	typ   int
	key   []byte
	value []byte
	arg   interface{}
}

type modifyRequest struct {
	cmp                btreeKeyComparator
	actions            []modifyAction
	fetchCallback      btreeFetchCallback
	reduce             reduceFunc
	rereduce           reduceFunc
	reduceContext      interface{}
	kvChunkThreshold   int
	kpChunkThreshold   int
	compacting         bool
	purgeKP            btreePurgeKPFunc
	purgeKV            btreePurgeKVFunc
	enablePurging      bool
	guidedPurgeContext interface{}
}

type modifyResult struct {
	request     *modifyRequest
	values      *nodeList
	valuesEnd   *nodeList
	nodeLen     int64
	count       int
	pointers    *nodeList
	pointersEnd *nodeList
	modified    bool
	nodeType    int
	errState    int
}

func (g *Gouchstore) addDocToUpdateList(doc *Document, docInfo *DocumentInfo, seq uint64) ([]byte, []byte, []byte, []byte, error) {
	updated := *docInfo
	updated.Seq = seq
	seqterm := encode_raw48(seq)

	if doc != nil {
		var diskSize uint64

		err := g.writeDoc(doc, &updated.bodyPosition, &diskSize, docInfo.compressed())
		if err != nil {
			return nil, nil, nil, nil, err
		}

		updated.Size = diskSize
	} else {
		updated.Deleted = true
		updated.bodyPosition = 0
		updated.Size = 0
	}

	idterm := []byte(updated.ID)
	seqval := updated.encodeBySeq()
	idval := updated.encodeById()

	return seqterm, idterm, seqval, idval, nil
}

func (g *Gouchstore) writeDoc(doc *Document, bp *uint64, diskSize *uint64, compress bool) error {
	var err error
	var pos, size int64
	if compress {
		pos, size, err = g.writeCompressedChunk(doc.Body)
	} else {
		pos, size, err = g.writeChunk(doc.Body, false)
	}
	if err != nil {
		return err
	}
	if bp != nil {
		*bp = uint64(pos)
	}
	if diskSize != nil {
		*diskSize = uint64(size)
	}
	return nil
}

func idFetchUpdate(req *modifyRequest, k []byte, v []byte, context interface{}) {
	indexUpdateContext := context.(*indexUpdateContext)
	if v == nil {
		return // doc not found
	}

	raw := DocumentInfo{}
	decodeByIdValue(&raw, v)
	oldseq := raw.Seq

	indexUpdateContext.seqacts[indexUpdateContext.actpos].typ = gs_ACTION_REMOVE
	indexUpdateContext.seqacts[indexUpdateContext.actpos].value = nil
	indexUpdateContext.seqacts[indexUpdateContext.actpos].key = encode_raw48(oldseq)
	indexUpdateContext.actpos++
}

func (g *Gouchstore) updateIndexes(seqs, seqvals, ids, idvals [][]byte) error {
	var fetcharg indexUpdateContext
	var seqrq, idrq modifyRequest

	numdocs := len(seqs)
	idacts := make([]modifyAction, 2*numdocs)
	seqacts := make([]modifyAction, 2*numdocs)

	fetcharg.seqacts = seqacts
	fetcharg.actpos = 0
	fetcharg.seqs = seqs
	fetcharg.seqvals = seqvals
	fetcharg.valpos = 0

	// sort the ids
	sortedIds := idAndValueList{
		ids:  ids,
		vals: idvals,
	}
	sort.Sort(sortedIds)

	for i := 0; i < numdocs; i++ {
		idacts[i*2].typ = gs_ACTION_FETCH
		idacts[(i * 2)].key = ids[i]
		idacts[i*2].arg = &fetcharg
		idacts[(i*2)+1].typ = gs_ACTION_INSERT
		idacts[(i*2)+1].key = ids[i]
		idacts[(i*2)+1].value = idvals[i]
	}

	idrq.cmp = gouchstoreIdComparator
	idrq.actions = idacts
	idrq.reduce = byIdReduce
	idrq.rereduce = byIdReReduce
	idrq.fetchCallback = idFetchUpdate
	idrq.compacting = false
	idrq.enablePurging = false
	idrq.purgeKP = nil
	idrq.purgeKV = nil
	idrq.kpChunkThreshold = gs_DB_CHUNK_THRESHOLD
	idrq.kvChunkThreshold = gs_DB_CHUNK_THRESHOLD

	newIdRoot, err := g.modifyBtree(&idrq, g.header.byIdRoot)
	if err != nil {
		return err
	}

	for fetcharg.valpos < numdocs {
		seqacts[fetcharg.actpos].typ = gs_ACTION_INSERT
		seqacts[fetcharg.actpos].value = seqvals[fetcharg.valpos]
		seqacts[fetcharg.actpos].key = seqs[fetcharg.valpos]
		fetcharg.valpos++
		fetcharg.actpos++
	}

	// we need to resize seqacts, as it was oversized intially
	// which leaves around invalid actions
	// FIXME evaluate for cleaner/faster way to do this
	seqacts = append([]modifyAction{}, seqacts[:fetcharg.actpos]...)
	sortedSeqActs := seqModifyActionList(seqacts)
	sort.Sort(sortedSeqActs)

	seqrq.cmp = gouchstoreSeqComparator
	seqrq.actions = seqacts
	seqrq.reduce = bySeqReduce
	seqrq.rereduce = bySeqReReduce
	seqrq.compacting = false
	seqrq.enablePurging = false
	seqrq.purgeKP = nil
	seqrq.purgeKV = nil
	seqrq.kpChunkThreshold = gs_DB_CHUNK_THRESHOLD
	seqrq.kvChunkThreshold = gs_DB_CHUNK_THRESHOLD

	newSeqRoot, err := g.modifyBtree(&seqrq, g.header.bySeqRoot)
	if err != nil {
		return err
	}

	if g.header.byIdRoot != newIdRoot {
		g.header.byIdRoot = newIdRoot
	}

	if g.header.bySeqRoot != newSeqRoot {
		g.header.bySeqRoot = newSeqRoot
	}

	return nil
}

func (g *Gouchstore) modifyBtree(req *modifyRequest, np *nodePointer) (*nodePointer, error) {
	var retPtr *nodePointer = np
	var err error
	rootResult := makeModifyResult(req)
	rootResult.nodeType = gs_KP_NODE

	err = g.modifyNode(req, np, 0, len(req.actions), rootResult)
	if err != nil {
		return nil, err
	}

	val := rootResult.values
	for val != nil {
		val = val.next
	}

	if rootResult.valuesEnd.pointer == np {
		//If we got the root pointer back, remove it from the list
		//so we don't try to free it.
		rootResult.valuesEnd.pointer = nil
	}

	if rootResult.modified {
		if rootResult.count > 1 || rootResult.pointers != rootResult.pointersEnd {
			//The root was split
			//Write it to disk and return the pointer to it.
			retPtr, err = g.finishRoot(req, rootResult)
			if err != nil {
				return nil, err
			}
		} else {
			retPtr = rootResult.valuesEnd.pointer
		}
	}

	return retPtr, nil
}

func (g *Gouchstore) modifyNode(req *modifyRequest, np *nodePointer, start, end int, dst *modifyResult) error {
	var nodebuf []byte
	var err error
	var bufpos int = 1
	var nodebuflen int = 0
	if start == end {
		return nil
	}

	if np != nil {
		nodebuf, err = g.readCompressedDataChunkAt(int64(np.pointer))
		if err != nil {
			return err
		}
		nodebuflen = len(nodebuf)
	}

	localResult := makeModifyResult(req)
	val := localResult.values
	for val != nil {
		val = val.next
	}

	if np == nil || nodebuf[0] == 1 { // KV Node
		localResult.nodeType = gs_KV_NODE
		for bufpos < nodebuflen {
			var cmpKey, valBuf []byte
			cmpKey, valBuf, bufpos = decodeKeyValue(nodebuf, bufpos)
			advance := false
			for !advance && start < end {
				advance = true
				cmpVal := req.cmp(cmpKey, req.actions[start].key)

				if cmpVal < 0 { // Key less than action key
					err = g.maybePurgeKV(req, cmpKey, valBuf, localResult)
					if err != nil {
						return err
					}
				} else if cmpVal > 0 { // Key greater than action key
					switch req.actions[start].typ {
					case gs_ACTION_INSERT:
						localResult.modified = true
						g.mrPushItem(req.actions[start].key, req.actions[start].value, localResult)
					case gs_ACTION_REMOVE:
						localResult.modified = true
					case gs_ACTION_FETCH:
						if req.fetchCallback != nil {
							// not found
							req.fetchCallback(req, req.actions[start].key, nil, req.actions[start].arg)
						}
					}
					start++
					// Do next action on same item in the node, as our action was
					// not >= it.
					advance = false
				} else if cmpVal == 0 { // Node key is equal to action key
					switch req.actions[start].typ {
					case gs_ACTION_INSERT:
						localResult.modified = true
						g.mrPushItem(req.actions[start].key, req.actions[start].value, localResult)
					case gs_ACTION_REMOVE:
						localResult.modified = true
					case gs_ACTION_FETCH:
						if req.fetchCallback != nil {
							req.fetchCallback(req, req.actions[start].key, valBuf, req.actions[start].arg)
						}
						// Do next action on same item in the node, as our action was a fetch
						// and there may be an equivalent insert or remove
						// following.
						advance = false
					}
					start++
				}
			}
			if start == end && !advance {
				// If we've exhausted actions then just keep this key
				err = g.maybePurgeKV(req, cmpKey, valBuf, localResult)
				if err != nil {
					return err
				}
			}
		}
		for start < end {
			// We're at the end of a leaf node.
			switch req.actions[start].typ {
			case gs_ACTION_INSERT:
				localResult.modified = true
				g.mrPushItem(req.actions[start].key, req.actions[start].value, localResult)
			case gs_ACTION_REMOVE:
				localResult.modified = true
			case gs_ACTION_FETCH:
				if req.fetchCallback != nil {
					// not found
					req.fetchCallback(req, req.actions[start].key, nil, req.actions[start].arg)
				}
			}
			start++
		}
	} else if nodebuf[0] == 0 { // KP Node
		localResult.nodeType = gs_KP_NODE
		for bufpos < nodebuflen && start < end {
			var cmpKey, valBuf []byte
			cmpKey, valBuf, bufpos = decodeKeyValue(nodebuf, bufpos)
			cmpVal := req.cmp(cmpKey, req.actions[start].key)
			if bufpos == nodebuflen {
				// We're at the last item in the kpnode, must apply all our
				// actions here.
				desc := decodeNodePointer(valBuf)
				desc.key = cmpKey

				err = g.modifyNode(req, desc, start, end, localResult)
				if err != nil {
					return nil
				}
				break
			}

			if cmpVal < 0 {
				// Key in node item less than action item and not at end
				// position, so just add it and continue.
				add := decodeNodePointer(valBuf)
				add.key = cmpKey

				err := g.maybePurgeKP(req, add, localResult)
				if err != nil {
					return err
				}
			} else if cmpVal >= 0 {
				// Found a key in the node greater than the one in the current
				// action. Descend into the pointed node with as many actions as
				// are less than the key here.
				rangeEnd := start
				for rangeEnd < end && req.cmp(req.actions[rangeEnd].key, cmpKey) <= 0 {
					rangeEnd++
				}

				desc := decodeNodePointer(valBuf)
				desc.key = cmpKey

				err := g.modifyNode(req, desc, start, rangeEnd, localResult)
				start = rangeEnd
				if err != nil {
					return err
				}
			}
		}
		for bufpos < nodebuflen {
			var cmpKey, valBuf []byte
			cmpKey, valBuf, bufpos = decodeKeyValue(nodebuf, bufpos)
			add := decodeNodePointer(valBuf)
			add.key = cmpKey

			err := g.maybePurgeKP(req, add, localResult)
			if err != nil {
				return err
			}
		}
	} else {
		return gs_ERROR_CORRUPT
	}
	val = localResult.values
	for val != nil {
		val = val.next
	}
	// If we've done modifications, write out the last leaf node.
	err = g.flushMR(localResult)
	if err != nil {
		return err
	}
	val = localResult.values
	for val != nil {
		val = val.next
	}
	ptr := localResult.pointers
	for ptr != nil {
		ptr = ptr.next
	}
	if !localResult.modified && np != nil {
		// If we didn't do anything, give back the pointer to the original
		g.mrPushPointerInfo(np, dst)
	} else {
		// Otherwise, give back the pointers to the nodes we've created.
		dst.modified = true
		err = g.mrMovePointers(localResult, dst)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Gouchstore) mrMovePointers(src *modifyResult, dst *modifyResult) error {
	var err error
	if src.pointersEnd == src.pointers {
		return err
	}

	ptr := src.pointers.next
	next := ptr
	for ptr != nil && err == nil {
		dst.nodeLen += int64(len(ptr.data) + len(ptr.key) + gs_KEY_VALUE_LEN)
		dst.count++

		next = ptr.next
		ptr.next = nil

		dst.valuesEnd.next = ptr
		dst.valuesEnd = ptr
		ptr = next
		err = g.maybeFlush(dst)
	}

	src.pointers.next = next
	src.pointersEnd = src.pointers
	return err
}

// Write a node using enough items from the values list to create a node
// with uncompressed size of at least mr_quota
func (g *Gouchstore) flushMRPartial(res *modifyResult, quota int64) error {
	var err error
	itemCount := 0
	nodebuf := new(bytes.Buffer)
	var subtreesize uint64 = 0
	var diskpos int64 = 0
	var disksize int64 = 0
	var finalKey []byte
	var reduceBuf []byte

	if !res.modified || res.valuesEnd == res.values {
		//Empty
		return nil
	}

	nodebuf.Write([]byte{byte(res.nodeType)})

	i := res.values.next

	//We don't care that we've reached mr_quota if we haven't written out
	//at least two items and we're not writing a leaf node.
	for i != nil && (quota > 0 || itemCount < 2 && res.nodeType == gs_KP_NODE) {
		nodebuf.Write(encodeKeyValue(i.key, i.data))
		if i.pointer != nil {
			subtreesize += i.pointer.subtreeSize
		}
		quota -= int64(len(i.key) + len(i.data) + gs_KEY_VALUE_LEN)
		finalKey = i.key
		i = i.next
		res.count--
		itemCount++
	}

	diskpos, disksize, err = g.writeCompressedChunk(nodebuf.Bytes())
	if err != nil {
		return err
	}

	if res.nodeType == gs_KV_NODE && res.request.reduce != nil {
		reduceBuf, err = res.request.reduce(res.values.next, itemCount, res.request.reduceContext)
		if err != nil {
			return err
		}
	}

	if res.nodeType == gs_KP_NODE && res.request.rereduce != nil {
		reduceBuf, err = res.request.rereduce(res.values.next, itemCount, res.request.reduceContext)
		if err != nil {
			return err
		}
	}

	// build new node pointer
	ptr := &nodePointer{
		key:          finalKey,
		pointer:      uint64(diskpos),
		reducedValue: reduceBuf,
		subtreeSize:  subtreesize + uint64(disksize),
	}

	// encode it as node list
	rawBytes := ptr.encode()
	pel := makeNodeList()
	pel.data = rawBytes
	pel.key = ptr.key
	pel.pointer = ptr

	res.pointersEnd.next = pel
	res.pointersEnd = pel

	res.nodeLen -= int64(nodebuf.Len() - 1) // FIXME why -1 here?

	res.values.next = i
	if i == nil {
		res.valuesEnd = res.values
	}

	return nil
}

func (g *Gouchstore) maybeFlush(mr *modifyResult) error {
	if mr.request.compacting {
		/* The compactor can (and should), just write out nodes
		 * of size CHUNK_SIZE as soon as it can, so that it can
		 * free memory it no longer needs. */
		if mr.modified &&
			(((mr.nodeType == gs_KV_NODE) && mr.nodeLen > int64(mr.request.kvChunkThreshold*2/3)) ||
				((mr.nodeType == gs_KP_NODE) && mr.nodeLen > int64(mr.request.kpChunkThreshold*2/3))) {
			return g.flushMR(mr)
		}

	} else if mr.modified && mr.count > 3 {
		/* Don't write out a partial node unless we've collected
		 * at least three items */
		if mr.nodeType == gs_KV_NODE && mr.nodeLen > int64(mr.request.kvChunkThreshold) {
			return g.flushMRPartial(mr, int64(mr.request.kvChunkThreshold*2/3))
		}
		if mr.nodeType == gs_KP_NODE && mr.nodeLen > int64(mr.request.kpChunkThreshold) {
			return g.flushMRPartial(mr, int64(mr.request.kpChunkThreshold*2/3))
		}
	}
	return nil
}

func (g *Gouchstore) mrPushPointerInfo(ptr *nodePointer, dst *modifyResult) error {
	rawBytes := ptr.encode()
	pel := makeNodeList()
	pel.data = rawBytes
	pel.key = ptr.key
	pel.pointer = ptr

	dst.valuesEnd.next = pel
	dst.valuesEnd = pel
	dst.nodeLen += int64(len(pel.key) + len(pel.data) + gs_KEY_VALUE_LEN)
	dst.count++

	return g.maybeFlush(dst)
}

func (g *Gouchstore) flushMR(res *modifyResult) error {
	return g.flushMRPartial(res, res.nodeLen)
}

func (g *Gouchstore) mrPushItem(key, val []byte, dst *modifyResult) error {
	itm := makeNodeList()
	itm.key = key
	itm.data = val
	itm.pointer = nil
	dst.valuesEnd.next = itm
	dst.valuesEnd = itm
	// Encoded size (see flush_mr)
	dst.nodeLen += int64(len(key) + len(val) + gs_KEY_VALUE_LEN)
	dst.count++
	return g.maybeFlush(dst)
}

func (g *Gouchstore) finishRoot(req *modifyRequest, rootResult *modifyResult) (*nodePointer, error) {
	var retPtr *nodePointer
	collector := makeModifyResult(req)
	collector.modified = true
	collector.nodeType = gs_KP_NODE
	g.flushMR(rootResult)
	for {
		if rootResult.pointersEnd == rootResult.pointers.next {
			// The root result split into exactly one kp_node.
			// Return the pointer to it.
			retPtr = rootResult.pointersEnd.pointer
			break
		} else {
			// The root result split into more than one kp_node.
			// Move the pointer list to the value list and write out the new node.
			err := g.mrMovePointers(rootResult, collector)
			if err != nil {
				return nil, err
			}

			err = g.flushMR(collector)
			if err != nil {
				return nil, err
			}

			// Swap root_result and collector mr's.
			tmp := rootResult
			rootResult = collector
			collector = tmp
		}
	}
	return retPtr, nil
}

// Perform purging for a kv-node if it qualifies for purging
func (g *Gouchstore) maybePurgeKV(req *modifyRequest, key, val []byte, res *modifyResult) error {
	var action int
	var err error
	if req.enablePurging && req.purgeKV != nil {
		action = req.purgeKV(key, val, req.guidedPurgeContext)
		if action < 0 {
			return fmt.Errorf("purge action failed: %d", action)
		}
	} else {
		action = gs_PURGE_KEEP
	}

	switch action {
	case gs_PURGE_ITEM:
		res.modified = true
	case gs_PURGE_STOP:
		req.enablePurging = false
	case gs_PURGE_KEEP:
		err = g.mrPushItem(key, val, res)
	}
	return err
}

func (g *Gouchstore) maybePurgeKP(req *modifyRequest, np *nodePointer, res *modifyResult) error {
	var action int
	var err error
	if req.enablePurging && req.purgeKP != nil {
		action = req.purgeKP(np, req.guidedPurgeContext)
		if action < 0 {
			return fmt.Errorf("purge action failed: %d", action)
		}
	} else {
		action = gs_PURGE_KEEP
	}

	switch action {
	case gs_PURGE_ITEM:
		res.modified = true
	case gs_PURGE_PARTIAL:
		err = g.purgeNode(req, np, res)
	case gs_PURGE_STOP:
		req.enablePurging = false
	case gs_PURGE_KEEP:
		err = g.mrPushPointerInfo(np, res)
	}
	return err
}

func (g *Gouchstore) purgeNode(req *modifyRequest, np *nodePointer, dst *modifyResult) error {
	var err error
	var nodebuf []byte
	var localResult *modifyResult
	bufpos := 1
	nodebuflen := 0

	if np == nil {
		return err
	}

	// noop: add back current node to destination
	if !req.enablePurging {
		dst.modified = true
		return g.mrPushPointerInfo(np, dst)
	}

	nodebuf, err = g.readCompressedDataChunkAt(int64(np.pointer))
	if err != nil {
		return err
	}
	nodebuflen = len(nodebuf)

	localResult = makeModifyResult(req)
	if nodebuf[0] == 1 { //KV Node
		localResult.nodeType = gs_KV_NODE
		for bufpos < nodebuflen {
			var cmpKey, valBuf []byte
			cmpKey, valBuf, bufpos = decodeKeyValue(nodebuf, bufpos)
			err = g.maybePurgeKV(req, cmpKey, valBuf, localResult)
			if err != nil {
				return err
			}
		}

	} else if nodebuf[0] == 0 { //KP Node
		localResult.nodeType = gs_KP_NODE
		for bufpos < nodebuflen {
			cmpKey, valBuf, posOffset := decodeKeyValue(nodebuf, bufpos)
			bufpos += posOffset

			desc := decodeNodePointer(valBuf)
			desc.key = cmpKey

			err = g.maybePurgeKP(req, desc, localResult)
			if err != nil {
				return err
			}
		}

	} else {
		return gs_ERROR_CORRUPT
	}

	// Write out changes and add node back to parent
	if localResult.modified {
		err = g.flushMR(localResult)
		if err != nil {
			return err
		}
		dst.modified = true
		err = g.mrMovePointers(localResult, dst)
		if err != nil {
			return err
		}
	}

	return nil
}

func makeModifyResult(req *modifyRequest) *modifyResult {
	v := makeNodeList()
	p := makeNodeList()
	rv := modifyResult{
		values:      v,
		valuesEnd:   v,
		pointers:    p,
		pointersEnd: p,
		nodeLen:     0,
		request:     req,
	}
	return &rv
}

func newBtreeModifyResult(cmp btreeKeyComparator, reduce reduceFunc, rereduce reduceFunc, reduceContext interface{}, kvChunkThreshold, kpChunkThreshold int) *modifyResult {
	rq := modifyRequest{
		cmp:              cmp,
		actions:          []modifyAction{},
		fetchCallback:    nil,
		reduce:           reduce,
		rereduce:         rereduce,
		reduceContext:    reduceContext,
		compacting:       true,
		enablePurging:    false,
		purgeKP:          nil,
		purgeKV:          nil,
		kvChunkThreshold: kvChunkThreshold,
		kpChunkThreshold: kpChunkThreshold,
	}

	mr := makeModifyResult(&rq)
	mr.modified = true
	mr.nodeType = gs_KV_NODE

	return mr
}

func makeNodeList() *nodeList {
	rv := nodeList{}
	return &rv
}

func (g *Gouchstore) completeNewBtree(mr *modifyResult) (*nodePointer, error) {
	err := g.flushMR(mr)
	if err != nil {
		return nil, err
	}

	targMr := makeModifyResult(mr.request)
	targMr.modified = true
	targMr.nodeType = gs_KP_NODE

	err = g.mrMovePointers(mr, targMr)
	if err != nil {
		return nil, err
	}

	var retPtr *nodePointer
	if targMr.count > 1 || targMr.pointers != targMr.pointersEnd {
		retPtr, err = g.finishRoot(mr.request, targMr)
		if err != nil {
			return nil, err
		}
	} else {
		retPtr = targMr.valuesEnd.pointer
	}

	return retPtr, nil
}
