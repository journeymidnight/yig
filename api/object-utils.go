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

package api

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// validBucket regexp.
var validBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$`)

// IsValidBucketName verifies a bucket name in accordance with Amazon's
// requirements. It must be 3-63 characters long, can contain dashes
// and periods, but must begin and end with a lowercase letter or a number.
// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
func isValidBucketName(bucketName string) bool {
	if !validBucket.MatchString(bucketName) {
		return false
	}
	// make sure there're no continuous dots
	if strings.Contains(bucketName, "..") {
		return false
	}
	// make sure it's not an IP address
	split := strings.Split(bucketName, ".")
	if len(split) == 4 {
		for _, p := range split {
			n, err := strconv.Atoi(p)
			if err == nil && n >= 0 && n <= 255 {
				return false
			}
		}
	}
	return true
}

// IsValidObjectName verifies an object name in accordance with Amazon's
// requirements. It cannot exceed 1024 characters and must be a valid UTF8
// string.
// Some characters require special handling:
// & $ @ = ; : + (space) , ?
// and ASCII ranges 0x00-0x1F(0-31 decimal) and 7F(127 decimal)
// Some characters to avoid:
// \ { ^ } % ` [ ] ' " < > ~ # |
// and non-printable ASCII characters(128-255 decimal)
//
// As in YIG, we PROHIBIT ALL the characters listed above
// See http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func isValidObjectName(objectName string) bool {
	if len(objectName) <= 0 || len(objectName) > 1024 {
		return false
	}
	if !utf8.ValidString(objectName) {
		return false
	}
	for _, n := range objectName {
		if (n >= 0 && n <= 31) || (n >= 127 && n <= 255) {
			return false
		}
		c := string(n)
		if strings.ContainsAny(c, "\\") {
			return false
		}
	}
	return true
}

// Argument position in handler AppendObject must be non-negative integer
func checkPosition(position string) (uint64, error) {
	p, err := strconv.ParseUint(position, 10, 64)
	if err != nil {
		return 0, err
	}
	if p < 0 {
		return 0, fmt.Errorf("position must ben on-negative integer.")
	}
	return p, nil
}

func isFirstAppend(position uint64) bool {
	return position == 0
}
