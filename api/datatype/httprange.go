/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package datatype

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	byteRangePrefix = "bytes="
)

// Valid byte position regexp
var validBytePos = regexp.MustCompile(`^[0-9]+$`)

// ErrorInvalidRange - returned when given range value is not valid.
var ErrorInvalidRange = errors.New("Invalid range")

// HttpRange specifies the byte range to be sent to the client.
type HttpRange struct {
	OffsetBegin  int64
	OffsetEnd    int64
	ResourceSize int64
}

// String populate range stringer interface
func (hrange HttpRange) String() string {
	return fmt.Sprintf("bytes %d-%d/%d", hrange.OffsetBegin, hrange.OffsetEnd, hrange.ResourceSize)
}

// getlength - get length from the range.
func (hrange HttpRange) GetLength() int64 {
	return 1 + hrange.OffsetEnd - hrange.OffsetBegin
}

func ParseRequestRange(rangeString string, resourceSize int64) (hrange *HttpRange, err error) {
	// TODO handle multi-range request
	// see https://tools.ietf.org/html/rfc7233

	// Return error if given range string doesn't start with byte range prefix.
	if !strings.HasPrefix(rangeString, byteRangePrefix) {
		return nil, fmt.Errorf("'%s' does not start with '%s'", rangeString, byteRangePrefix)
	}

	// Trim byte range prefix.
	byteRangeString := strings.TrimPrefix(rangeString, byteRangePrefix)

	// Check if range string contains delimiter '-', else return error. eg. "bytes=8"
	sepIndex := strings.Index(byteRangeString, "-")
	if sepIndex == -1 {
		return nil, fmt.Errorf("'%s' does not have a valid range value", rangeString)
	}

	offsetBeginString := byteRangeString[:sepIndex]
	offsetBegin := int64(-1)
	// Convert offsetBeginString only if its not empty.
	if len(offsetBeginString) > 0 {
		if !validBytePos.MatchString(offsetBeginString) {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}

		if offsetBegin, err = strconv.ParseInt(offsetBeginString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid first byte position value", rangeString)
		}
	}

	offsetEndString := byteRangeString[sepIndex+1:]
	offsetEnd := int64(-1)
	// Convert offsetEndString only if its not empty.
	if len(offsetEndString) > 0 {
		if !validBytePos.MatchString(offsetEndString) {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}

		if offsetEnd, err = strconv.ParseInt(offsetEndString, 10, 64); err != nil {
			return nil, fmt.Errorf("'%s' does not have a valid last byte position value", rangeString)
		}
	}

	// rangeString contains first and last byte positions. eg. "bytes=2-5"
	if offsetBegin > -1 && offsetEnd > -1 {
		if offsetBegin > offsetEnd {
			// Last byte position is not greater than first byte position. eg. "bytes=5-2"
			return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
		}

		// First and last byte positions should not be >= resourceSize.
		if offsetBegin >= resourceSize {
			return nil, ErrorInvalidRange
		}

		if offsetEnd >= resourceSize {
			offsetEnd = resourceSize - 1
		}
	} else if offsetBegin > -1 {
		// rangeString contains only first byte position. eg. "bytes=8-"
		if offsetBegin >= resourceSize {
			// First byte position should not be >= resourceSize.
			return nil, ErrorInvalidRange
		}

		offsetEnd = resourceSize - 1
	} else if offsetEnd > -1 {
		// rangeString contains only last byte position. eg. "bytes=-3"
		if offsetEnd == 0 {
			// Last byte position should not be zero eg. "bytes=-0"
			return nil, ErrorInvalidRange
		}

		if offsetEnd >= resourceSize {
			offsetBegin = 0
		} else {
			offsetBegin = resourceSize - offsetEnd
		}

		offsetEnd = resourceSize - 1
	} else {
		// rangeString contains first and last byte positions missing. eg. "bytes=-"
		return nil, fmt.Errorf("'%s' does not have valid range value", rangeString)
	}

	return &HttpRange{offsetBegin, offsetEnd, resourceSize}, nil
}
