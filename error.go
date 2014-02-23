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
)

var gs_ERROR_INVALID_ARGUMENTS = fmt.Errorf("invalid arguments")
var gs_ERROR_INVALID_CHUNK_SHORT_PREFIX = fmt.Errorf("invalid chunk, prefix too short")
var gs_ERROR_INVALID_CHUNK_SIZE_TOO_SMALL = fmt.Errorf("invalid chunk, chunk size too small")
var gs_ERROR_INVALID_CHUNK_DATA_LESS_THAN_SIZE = fmt.Errorf("invalid chunk, data less than size")
var gs_ERROR_INVALID_CHUNK_BAD_CRC = fmt.Errorf("invalid chunk, bad crc")

var gs_ERROR_INVALID_HEADER_BAD_SIZE = fmt.Errorf("invalid header, bad size")

var gs_ERROR_INVALID_BTREE_NODE_TYPE = fmt.Errorf("invalid btree node, bad type")

var gs_ERROR_DOCUMENT_NOT_FOUND = fmt.Errorf("document not found")

var gs_ERROR_CORRUPT = fmt.Errorf("corrupt")
