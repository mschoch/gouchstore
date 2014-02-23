package gouchstore

import (
	"bytes"
)

type reduceFunc func(leaflist *nodeList, count int, context interface{}) ([]byte, error)

func byIdReduce(leaflist *nodeList, count int, context interface{}) ([]byte, error) {
	var notDeleted, deleted, size uint64
	i := leaflist
	for i != nil && count > 0 {
		docinfo := DocumentInfo{}
		decodeByIdValue(&docinfo, i.data)
		if docinfo.Deleted {
			deleted++
		} else {
			notDeleted++
		}
		size += docinfo.Size
		i = i.next
		count--
	}
	return encodeByIdReduce(notDeleted, deleted, size), nil
}

func byIdReReduce(leaflist *nodeList, count int, context interface{}) ([]byte, error) {
	var notDeleted, deleted, size uint64
	i := leaflist
	for i != nil && count > 0 {
		if i.pointer != nil {
			nd, d, s := decodeByIdReduce(i.pointer.reducedValue)
			notDeleted += nd
			deleted += d
			size += s
		}
		i = i.next
		count--
	}
	return encodeByIdReduce(notDeleted, deleted, size), nil
}

func encodeByIdReduce(notDeleted, deleted, size uint64) []byte {
	buf := new(bytes.Buffer)
	buf.Write(encode_raw40(notDeleted))
	buf.Write(encode_raw40(deleted))
	buf.Write(encode_raw48(size))
	return buf.Bytes()
}

func decodeByIdReduce(buf []byte) (uint64, uint64, uint64) {
	notDeleted := decode_raw40(buf[0:5])
	deleted := decode_raw40(buf[5:10])
	size := decode_raw48(buf[10:16])
	return notDeleted, deleted, size
}

func bySeqReduce(leaflist *nodeList, count int, context interface{}) ([]byte, error) {
	return encode_raw40(uint64(count)), nil
}

func bySeqReReduce(leaflist *nodeList, count int, context interface{}) ([]byte, error) {
	var total uint64
	i := leaflist
	for i != nil && count > 0 {
		if i.pointer != nil {
			t := decode_raw40(i.pointer.reducedValue)
			total += t
		}
		i = i.next
		count--
	}
	return encode_raw40(total), nil
}
