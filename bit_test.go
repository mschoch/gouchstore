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
	"testing"
)

func TestDecodeTwelveTwentyEightSplit(t *testing.T) {
	tests := []struct {
		input  []byte
		keyLen uint32
		valLen uint32
	}{
		{
			input:  []byte{0x01, 0x80, 0x00, 0x00, 0x1e},
			keyLen: 24,
			valLen: 30,
		},
	}

	for _, test := range tests {
		kl, vl := decode_raw_12_28_split(test.input)
		if kl != test.keyLen {
			t.Errorf("expected keyLen: %d, got %d", test.keyLen, kl)
		}
		if vl != test.valLen {
			t.Errorf("expected valLen: %d, got %d", test.valLen, vl)
		}

	}
}
