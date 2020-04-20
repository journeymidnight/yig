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
	"github.com/journeymidnight/yig/meta/common"
)

// NoncurrentDays is a type alias to unmarshal Days in NoncurrentVersionExpiration or NoncurrentVersionTransition
type NoncurrentDays int

// UnmarshalXML parses number of days and validates if greater than zero
func (nDays *NoncurrentDays) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var numDays int
	err := d.DecodeElement(&numDays, &startElement)
	if err != nil {
		return err
	}
	if numDays <= 0 {
		return ErrInvalidLcDays
	}
	*nDays = NoncurrentDays(numDays)
	return nil
}

// MarshalXML encodes number of days to expire if it is non-zero and
// encodes empty string otherwise
func (nDays *NoncurrentDays) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if *nDays == NoncurrentDays(0) {
		return nil
	}
	return e.EncodeElement(int(*nDays), startElement)
}

// NoncurrentVersionExpiration - an action for lifecycle configuration rule.
type NoncurrentVersionExpiration struct {
	XMLName        xml.Name       `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentDays NoncurrentDays `xml:"NoncurrentDays,omitempty"`
}

// NoncurrentVersionTransition - an action for lifecycle configuration rule.
type NoncurrentVersionTransition struct {
	XMLName        xml.Name       `xml:"NoncurrentVersionTransition,omitempty"`
	NoncurrentDays NoncurrentDays `xml:"NoncurrentDays,omitempty"`
	StorageClass   string         `xml:"StorageClass,omitempty"`
}

// Validate - validates the "NoncurrentVersionExpiration" element
func (n NoncurrentVersionExpiration) Validate() error {
	if n.IsDaysNull() {
		return ErrLcMissingNoncurrentDays
	}

	return nil
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionExpiration) IsDaysNull() bool {
	return n.NoncurrentDays == NoncurrentDays(0)
}

// Validate - validates the "NoncurrentVersionTransition" element
func (n NoncurrentVersionTransition) Validate() error {
	if n.XMLName.Local == "" {
		return nil
	}

	// NoncurrentDays is specified
	if n.IsDaysNull() {
		return ErrLcMissingNoncurrentDays
	}

	//StorageClass is specified
	if n.IsStorageClassNull() {
		return ErrLcMissingStorageClass
	}

	_, err := common.MatchStorageClassIndex(n.StorageClass)
	if err != nil {
		return ErrInvalidStorageClass
	}

	return nil
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionTransition) IsDaysNull() bool {
	return n.NoncurrentDays == NoncurrentDays(0)
}

// IsDaysNull returns true if days field is null
func (n NoncurrentVersionTransition) IsStorageClassNull() bool {
	return n.StorageClass == string("")
}
