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
)

type btreeKeyComparator func(a, b []byte) int

func gouchstoreIdComparator(a, b []byte) int {
	return bytes.Compare(a, b)
}

func gouchstoreSeqComparator(a, b []byte) int {

	aseq := decode_raw48(a)
	bseq := decode_raw48(b)

	if aseq < bseq {
		return -1
	} else if aseq == bseq {
		return 0
	}
	return 1
}

type seqList []uint64

func (s seqList) Len() int           { return len(s) }
func (s seqList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s seqList) Less(i, j int) bool { return s[i] < s[j] }

type idList [][]byte

func (idl idList) Len() int           { return len(idl) }
func (idl idList) Swap(i, j int)      { idl[i], idl[j] = idl[j], idl[i] }
func (idl idList) Less(i, j int) bool { return gouchstoreIdComparator(idl[i], idl[j]) < 0 }

// like idList, but capabable also sorts the values the same as the ids
type idAndValueList struct {
	ids  idList
	vals idList
}

func (idavl idAndValueList) Len() int { return idavl.ids.Len() }
func (idavl idAndValueList) Swap(i, j int) {
	idavl.ids.Swap(i, j)
	idavl.vals[i], idavl.vals[j] = idavl.vals[j], idavl.vals[i]
}
func (idavl idAndValueList) Less(i, j int) bool { return idavl.ids.Less(i, j) }

type seqModifyActionList []modifyAction

func (s seqModifyActionList) Len() int      { return len(s) }
func (s seqModifyActionList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s seqModifyActionList) Less(i, j int) bool {
	return gouchstoreSeqComparator(s[i].key, s[j].key) < 0
}
