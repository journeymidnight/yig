/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package lifecycle

import (
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
)

// Filter - a filter for a lifecycle configuration Rule.
type Filter struct {
	XMLName xml.Name `xml:"Filter"`
	Prefix  *string  `xml:"Prefix,omitempty"`
	And     *And     `xml:"And,omitempty"`
	Tag     *Tag     `xml:"Tag,omitempty"`
}

// Validate - validates the filter element
func (f Filter) Validate() error {
	// A Filter must have exactly one of Prefix, Tag, or And specified.
	if f.And != nil && !f.And.isEmpty() {
		if f.Prefix != nil {
			return ErrInvalidLcFilter
		}
		if f.Tag != nil {
			return ErrInvalidLcFilter
		}
		if err := f.And.Validate(); err != nil {
			return err
		}
	}
	if f.Prefix != nil {
		if f.Tag != nil {
			return ErrInvalidLcFilter
		}
	}
	if f.Tag != nil {
		if !f.Tag.IsEmpty() {
			if err := f.Tag.Validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

// IsTagEmpty - return true if Filter tag is empty
func (f Filter) IsTagEmpty() bool {
	if f.Tag != nil {
		return false
	}
	if f.And != nil && len(f.And.Tags) > 0 {
			return false
	}
	return true
}

// isEmpty - returns true if Filter is empty
func (f Filter) isEmpty() bool {
	return f.And.isEmpty() && f.Prefix == nil && f.Tag == nil
}

//String returns a pointer to the string value passed in.
func String(s string) *string {
	return &s
}