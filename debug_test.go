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
	"testing"
)

var expectedHeader = `Address is on a 4096 byte boundary...
Appears to be a header...
Header Found!
Header bytes 0b 00 00 00 00 00 65 00 00 00 00 00 00 00 00 00 00 00 00 00 11 00 1c 00 0c 00 00 00 00 90 0c 00 00 00 00 19 c3 00 00 00 00 65 00 00 00 00 a8 ce 00 00 00 00 19 e3 00 00 00 00 65 00 00 00 00 00 00 00 00 00 76 ea 00 00 00 03 80 5b 00 00 00 00 00 5d
Disk Version: 11 (0xb)
Update Seq: 101
Purge Seq: 0
Purge Pointer: 0 (0x0)
By Sequence Pointer: 36876 (0x900c)
By Sequence Subtree Size: 6595 (0x19c3)
By Sequence Reduced Count: 101
By ID Pointer: 43214 (0xa8ce)
By ID Subtree Size: 6627 (0x19e3)
By ID Reduced Document Count: 101
By ID Reduced Deleted Document Count: 0
By ID Reduced Size: 30442
Local Docs Pointer: 229467 (0x3805b)
Local Docs Subtree Size: 93 (0x5d)
`

func TestDebugHeader(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 0x39000, true, false, -1)

	if buffer.String() != expectedHeader {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedHeader, buffer.String())
	}
}

var expectedNotHeader = `Address is on a 4096 byte boundary...
Does not appear to be a header ( 36)
`

func TestDebugNotAHeader(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 0x9000, true, false, -1)

	if buffer.String() != expectedNotHeader {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedNotHeader, buffer.String())
	}
}

var expectedInterior = `Trying to read compressed chunk...
Appears to be an interior node...
Interior node found!
Interior Node: [
Key: 'brewery_belle_vue-gueuze' (62 72 65 77 65 72 79 5f 62 65 6c 6c 65 5f 76 75 65 2d 67 75 65 75 7a 65) Pointer: 37046 Subtree Size: 681 ReduceValue: 00 00 00 00 0c 00 00 00 00 00 00 00 00 00 0b ca,
Key: 'dick_s_brewing-pale_ale' (64 69 63 6b 5f 73 5f 62 72 65 77 69 6e 67 2d 70 61 6c 65 5f 61 6c 65) Pointer: 37727 Subtree Size: 663 ReduceValue: 00 00 00 00 0c 00 00 00 00 00 00 00 00 00 0b 75,
Key: 'firehouse_grill_brewery-pale_ale' (66 69 72 65 68 6f 75 73 65 5f 67 72 69 6c 6c 5f 62 72 65 77 65 72 79 2d 70 61 6c 65 5f 61 6c 65) Pointer: 38390 Subtree Size: 656 ReduceValue: 00 00 00 00 0b 00 00 00 00 00 00 00 00 00 0b f3,
Key: 'lakefront_brewery-bock' (6c 61 6b 65 66 72 6f 6e 74 5f 62 72 65 77 65 72 79 2d 62 6f 63 6b) Pointer: 39046 Subtree Size: 725 ReduceValue: 00 00 00 00 0c 00 00 00 00 00 00 00 00 00 0c f9,
Key: 'montana_brewing-sandbagger_gold' (6d 6f 6e 74 61 6e 61 5f 62 72 65 77 69 6e 67 2d 73 61 6e 64 62 61 67 67 65 72 5f 67 6f 6c 64) Pointer: 39771 Subtree Size: 693 ReduceValue: 00 00 00 00 0c 00 00 00 00 00 00 00 00 00 0f 9c,
Key: 'prescott_brewing_company' (70 72 65 73 63 6f 74 74 5f 62 72 65 77 69 6e 67 5f 63 6f 6d 70 61 6e 79) Pointer: 40464 Subtree Size: 689 ReduceValue: 00 00 00 00 0c 00 00 00 00 00 00 00 00 00 0e d4,
Key: 'san_marcos_brewery_grill-premium_golden_ale' (73 61 6e 5f 6d 61 72 63 6f 73 5f 62 72 65 77 65 72 79 5f 67 72 69 6c 6c 2d 70 72 65 6d 69 75 6d 5f 67 6f 6c 64 65 6e 5f 61 6c 65) Pointer: 41153 Subtree Size: 682 ReduceValue: 00 00 00 00 0a 00 00 00 00 00 00 00 00 00 0b cb,
Key: 'the_narragansett_brewing_company-narragansett_lager' (74 68 65 5f 6e 61 72 72 61 67 61 6e 73 65 74 74 5f 62 72 65 77 69 6e 67 5f 63 6f 6d 70 61 6e 79 2d 6e 61 72 72 61 67 61 6e 73 65 74 74 5f 6c 61 67 65 72) Pointer: 41835 Subtree Size: 760 ReduceValue: 00 00 00 00 0b 00 00 00 00 00 00 00 00 00 10 07,
Key: 'zea_rotisserie_and_brewery-clearview_light' (7a 65 61 5f 72 6f 74 69 73 73 65 72 69 65 5f 61 6e 64 5f 62 72 65 77 65 72 79 2d 63 6c 65 61 72 76 69 65 77 5f 6c 69 67 68 74) Pointer: 42595 Subtree Size: 619 ReduceValue: 00 00 00 00 09 00 00 00 00 00 00 00 00 00 0c 7d
]
`

func TestDebugInterior(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 0xa8ce, false, false, -1)

	if buffer.String() != expectedInterior {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedInterior, buffer.String())
	}
}

var expectedLeaf = `Trying to read compressed chunk...
Appears to be a leaf node...
Guessing this node is in the byId index
Leaf node found!
Leaf Node: [
ID: 'abita_brewing_company-s_o_s' Seq: 1 Rev: 1 Deleted: false Size: 314 BodyPosition: 1 (0x1),
ID: 'ali_i_brewing-kona_coffee_stout' Seq: 2 Rev: 1 Deleted: false Size: 219 BodyPosition: 315 (0x13b),
ID: 'amherst_brewing_company-bankers_gold' Seq: 3 Rev: 1 Deleted: false Size: 343 BodyPosition: 534 (0x216),
ID: 'aspen_brewing_company' Seq: 4 Rev: 1 Deleted: false Size: 253 BodyPosition: 877 (0x36d),
ID: 'belhaven_brewery-twisted_thistle_india_pale_ale' Seq: 5 Rev: 1 Deleted: false Size: 235 BodyPosition: 1130 (0x46a),
ID: 'bell_s_brewery_inc-two_hearted_ale' Seq: 6 Rev: 1 Deleted: false Size: 403 BodyPosition: 1365 (0x555),
ID: 'bellows_brew_crew-steam_lager' Seq: 7 Rev: 1 Deleted: false Size: 155 BodyPosition: 1768 (0x6e8),
ID: 'big_ridge_brewing-lager' Seq: 8 Rev: 1 Deleted: false Size: 205 BodyPosition: 1923 (0x783),
ID: 'brasserie_brouwerij_cantillon-iris_1996' Seq: 9 Rev: 1 Deleted: false Size: 243 BodyPosition: 2128 (0x850),
ID: 'brauerei_wieselburg' Seq: 10 Rev: 1 Deleted: false Size: 286 BodyPosition: 2371 (0x943),
ID: 'brauhaus_onuma-kolsch' Seq: 11 Rev: 1 Deleted: false Size: 149 BodyPosition: 2657 (0xa61),
ID: 'brewery_belle_vue-gueuze' Seq: 12 Rev: 1 Deleted: false Size: 213 BodyPosition: 2806 (0xaf6)
]
`

func TestDebugLeaf(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 37046, false, false, -1)

	if buffer.String() != expectedLeaf {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedLeaf, buffer.String())
	}
}

var expectedSeqInterior = `Trying to read compressed chunk...
Appears to be an interior node...
Interior node found!
Interior Node: [
Key: (00 00 00 00 00 0c) Pointer: 3019 Subtree Size: 719 ReduceValue: 00 00 00 00 0c,
Key: (00 00 00 00 00 18) Pointer: 6672 Subtree Size: 686 ReduceValue: 00 00 00 00 0c,
Key: (00 00 00 00 00 23) Pointer: 10418 Subtree Size: 684 ReduceValue: 00 00 00 00 0b,
Key: (00 00 00 00 00 2e) Pointer: 14212 Subtree Size: 712 ReduceValue: 00 00 00 00 0b,
Key: (00 00 00 00 00 3a) Pointer: 18974 Subtree Size: 715 ReduceValue: 00 00 00 00 0c,
Key: (00 00 00 00 00 46) Pointer: 23326 Subtree Size: 735 ReduceValue: 00 00 00 00 0c,
Key: (00 00 00 00 00 50) Pointer: 27234 Subtree Size: 684 ReduceValue: 00 00 00 00 0a,
Key: (00 00 00 00 00 5a) Pointer: 31112 Subtree Size: 740 ReduceValue: 00 00 00 00 0a,
Key: (00 00 00 00 00 65) Pointer: 36126 Subtree Size: 750 ReduceValue: 00 00 00 00 0b
]
`

func TestDebugSeqInterior(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 0x900c, false, false, -1)

	if buffer.String() != expectedSeqInterior {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedSeqInterior, buffer.String())
	}
}

var expectedSeqLeaf = `Trying to read compressed chunk...
raw chunk data: 01 00 60 00 00 3d 00 00 00 00 00 01 01 b0 00 01 3a 00 00 00 00 00 01 00 00 00 00 00 01 80 61 62 69 74 61 5f 62 72 65 77 69 6e 67 5f 63 6f 6d 70 61 6e 79 2d 73 5f 6f 5f 73 00 00 04 0c 43 a0 07 c0 00 00 00 00 00 00 00 00 00 60 00 00 41 00 00 00 00 00 02 01 f0 00 00 db 00 00 00 00 01 3b 00 00 00 00 00 01 80 61 6c 69 5f 69 5f 62 72 65 77 69 6e 67 2d 6b 6f 6e 61 5f 63 6f 66 66 65 65 5f 73 74 6f 75 74 00 00 04 0c 51 d2 7d ea 00 00 00 00 00 00 00 00 00 60 00 00 46 00 00 00 00 00 03 02 40 00 01 57 00 00 00 00 02 16 00 00 00 00 00 01 80 61 6d 68 65 72 73 74 5f 62 72 65 77 69 6e 67 5f 63 6f 6d 70 61 6e 79 2d 62 61 6e 6b 65 72 73 5f 67 6f 6c 64 00 00 04 0c 5b fe 6c 26 00 00 00 00 00 00 00 00 00 60 00 00 37 00 00 00 00 00 04 01 50 00 00 fd 00 00 00 00 03 6d 00 00 00 00 00 01 80 61 73 70 65 6e 5f 62 72 65 77 69 6e 67 5f 63 6f 6d 70 61 6e 79 00 00 04 0c 78 f1 65 1a 00 00 00 00 00 00 00 00 00 60 00 00 51 00 00 00 00 00 05 02 f0 00 00 eb 00 00 00 00 04 6a 00 00 00 00 00 01 80 62 65 6c 68 61 76 65 6e 5f 62 72 65 77 65 72 79 2d 74 77 69 73 74 65 64 5f 74 68 69 73 74 6c 65 5f 69 6e 64 69 61 5f 70 61 6c 65 5f 61 6c 65 00 00 04 0c 9d af 33 6d 00 00 00 00 00 00 00 00 00 60 00 00 44 00 00 00 00 00 06 02 20 00 01 93 00 00 00 00 05 55 00 00 00 00 00 01 80 62 65 6c 6c 5f 73 5f 62 72 65 77 65 72 79 5f 69 6e 63 2d 74 77 6f 5f 68 65 61 72 74 65 64 5f 61 6c 65 00 00 04 0c a1 cf 79 d7 00 00 00 00 00 00 00 00 00 60 00 00 3f 00 00 00 00 00 07 01 d0 00 00 9b 00 00 00 00 06 e8 00 00 00 00 00 01 80 62 65 6c 6c 6f 77 73 5f 62 72 65 77 5f 63 72 65 77 2d 73 74 65 61 6d 5f 6c 61 67 65 72 00 00 04 0c a2 6b e4 37 00 00 00 00 00 00 00 00 00 60 00 00 39 00 00 00 00 00 08 01 70 00 00 cd 00 00 00 00 07 83 00 00 00 00 00 01 80 62 69 67 5f 72 69 64 67 65 5f 62 72 65 77 69 6e 67 2d 6c 61 67 65 72 00 00 04 0c aa f2 14 49 00 00 00 00 00 00 00 00 00 60 00 00 49 00 00 00 00 00 09 02 70 00 00 f3 00 00 00 00 08 50 00 00 00 00 00 01 80 62 72 61 73 73 65 72 69 65 5f 62 72 6f 75 77 65 72 69 6a 5f 63 61 6e 74 69 6c 6c 6f 6e 2d 69 72 69 73 5f 31 39 39 36 00 00 04 0c d2 35 c5 2b 00 00 00 00 00 00 00 00 00 60 00 00 35 00 00 00 00 00 0a 01 30 00 01 1e 00 00 00 00 09 43 00 00 00 00 00 01 80 62 72 61 75 65 72 65 69 5f 77 69 65 73 65 6c 62 75 72 67 00 00 04 0c f5 fb 01 df 00 00 00 00 00 00 00 00 00 60 00 00 37 00 00 00 00 00 0b 01 50 00 00 95 00 00 00 00 0a 61 00 00 00 00 00 01 80 62 72 61 75 68 61 75 73 5f 6f 6e 75 6d 61 2d 6b 6f 6c 73 63 68 00 00 04 0c f8 71 80 3f 00 00 00 00 00 00 00 00 00 60 00 00 3a 00 00 00 00 00 0c 01 80 00 00 d5 00 00 00 00 0a f6 00 00 00 00 00 01 80 62 72 65 77 65 72 79 5f 62 65 6c 6c 65 5f 76 75 65 2d 67 75 65 75 7a 65 00 00 04 0d 06 6b 87 21 00 00 00 00 00 00 00 00
Appears to be a leaf node...
Guessing this node is in the bySeq index
Leaf node found!
Leaf Node: [
ID: 'abita_brewing_company-s_o_s' Seq: 1 Rev: 1 Deleted: false Size: 314 BodyPosition: 1 (0x1),
ID: 'ali_i_brewing-kona_coffee_stout' Seq: 2 Rev: 1 Deleted: false Size: 219 BodyPosition: 315 (0x13b),
ID: 'amherst_brewing_company-bankers_gold' Seq: 3 Rev: 1 Deleted: false Size: 343 BodyPosition: 534 (0x216),
ID: 'aspen_brewing_company' Seq: 4 Rev: 1 Deleted: false Size: 253 BodyPosition: 877 (0x36d),
ID: 'belhaven_brewery-twisted_thistle_india_pale_ale' Seq: 5 Rev: 1 Deleted: false Size: 235 BodyPosition: 1130 (0x46a),
ID: 'bell_s_brewery_inc-two_hearted_ale' Seq: 6 Rev: 1 Deleted: false Size: 403 BodyPosition: 1365 (0x555),
ID: 'bellows_brew_crew-steam_lager' Seq: 7 Rev: 1 Deleted: false Size: 155 BodyPosition: 1768 (0x6e8),
ID: 'big_ridge_brewing-lager' Seq: 8 Rev: 1 Deleted: false Size: 205 BodyPosition: 1923 (0x783),
ID: 'brasserie_brouwerij_cantillon-iris_1996' Seq: 9 Rev: 1 Deleted: false Size: 243 BodyPosition: 2128 (0x850),
ID: 'brauerei_wieselburg' Seq: 10 Rev: 1 Deleted: false Size: 286 BodyPosition: 2371 (0x943),
ID: 'brauhaus_onuma-kolsch' Seq: 11 Rev: 1 Deleted: false Size: 149 BodyPosition: 2657 (0xa61),
ID: 'brewery_belle_vue-gueuze' Seq: 12 Rev: 1 Deleted: false Size: 213 BodyPosition: 2806 (0xaf6)
]
`

func TestDebugSeqLeaf(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 3019, true, false, -1)

	if buffer.String() != expectedSeqLeaf {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedSeqLeaf, buffer.String())
	}
}

var expectedData = `Trying to read compressed chunk...
raw chunk data: 7b 22 6e 61 6d 65 22 3a 22 53 2e 4f 2e 53 22 2c 22 61 62 76 22 3a 30 2e 30 2c 22 69 62 75 22 3a 30 2e 30 2c 22 73 72 6d 22 3a 30 2e 30 2c 22 75 70 63 22 3a 30 2c 22 74 79 70 65 22 3a 22 62 65 65 72 22 2c 22 62 72 65 77 65 72 79 5f 69 64 22 3a 22 61 62 69 74 61 5f 62 72 65 77 69 6e 67 5f 63 6f 6d 70 61 6e 79 22 2c 22 75 70 64 61 74 65 64 22 3a 22 32 30 31 31 2d 30 38 2d 31 35 20 31 31 3a 34 38 3a 32 30 22 2c 22 64 65 73 63 72 69 70 74 69 6f 6e 22 3a 22 53 61 76 65 20 4f 75 72 20 53 68 6f 72 65 73 20 28 53 2e 4f 2e 53 29 20 69 73 20 61 6e 20 75 6e 66 69 6c 74 65 72 65 64 20 77 65 69 7a 65 6e 20 70 69 6c 73 20 61 6e 64 20 68 61 73 20 61 20 62 72 69 6c 6c 69 61 6e 74 20 67 6f 6c 64 65 6e 20 63 6f 6c 6f 72 2e 20 41 20 43 68 61 72 69 74 61 62 6c 65 20 50 69 6c 73 6e 65 72 22 2c 22 73 74 79 6c 65 22 3a 22 47 65 72 6d 61 6e 2d 53 74 79 6c 65 20 50 69 6c 73 65 6e 65 72 22 2c 22 63 61 74 65 67 6f 72 79 22 3a 22 47 65 72 6d 61 6e 20 4c 61 67 65 72 22 7d
Assuming data chunk!
`

func TestDebugData(t *testing.T) {
	db, err := Open(testFileName, OPEN_RDONLY)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buffer := new(bytes.Buffer)
	db.DebugAddress(buffer, 0x1, true, false, -1)

	if buffer.String() != expectedData {
		t.Errorf("expcted '%s'\n got '%s'\n", expectedData, buffer.String())
	}
}
