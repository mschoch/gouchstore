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
