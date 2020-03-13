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
	"bytes"
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
)

// Status represents lifecycle configuration status
type Status string

// Supported status types
const (
	Enabled  Status = "Enabled"
	Disabled Status = "Disabled"
)

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName                      xml.Name                      `xml:"Rule"`
	ID                           string                        `xml:"ID,omitempty"`
	Status                       Status                        `xml:"Status"`
	Filter                       *Filter                       `xml:"Filter,omitempty"`
	Expiration                   *Expiration                   `xml:"Expiration,omitempty"`
	Transitions                  []Transition                  `xml:"Transition,omitempty"`
	NoncurrentVersionExpiration  *NoncurrentVersionExpiration  `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransitions []NoncurrentVersionTransition `xml:"NoncurrentVersionTransition,omitempty"`
	// FIXME: add a type to catch unsupported AbortIncompleteMultipartUpload AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
}

// validateID - checks if ID is valid or not.
func (r Rule) validateID() error {
	// cannot be longer than 255 characters
	if len(string(r.ID)) > 255 {
		return ErrInvalidLcRuleID
	}
	return nil
}

// validateStatus - checks if status is valid or not.
func (r Rule) validateStatus() error {
	// Status can't be empty
	if len(r.Status) == 0 {
		return ErrInvalidLcRuleStatus
	}

	// Status must be one of Enabled or Disabled
	if r.Status != Enabled && r.Status != Disabled {
		return ErrInvalidLcRuleStatus
	}
	return nil
}

func (r Rule) validateAction() error {
	if r.Expiration != nil || len(r.Transitions) != 0 {
		if r.Expiration != nil {
			if err := r.Expiration.Validate(); err != nil {
				return err
			}
		}

		for _, transition := range r.Transitions {
			if err := transition.Validate(); err != nil {
				return err
			}
		}

		return nil
	}

	return ErrLcMissingAction
}

func (r Rule) validateNoncurrentVersion() error {
	if r.NoncurrentVersionExpiration != nil {
		if err := r.NoncurrentVersionExpiration.Validate(); err != nil {
			return err
		}
	}

	for _, nvTransition := range r.NoncurrentVersionTransitions {
		if err := nvTransition.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (r Rule) validateFilter() error {
	if r.Filter != nil {
		return r.Filter.Validate()
	}
	return nil
}

// Prefix - a rule can either have prefix under <filter></filter> or under
// <filter><and></and></filter>. This method returns the prefix from the
// location where it is available
func (r Rule) Prefix() string {
	if r.Filter.Prefix != nil {
		return *r.Filter.Prefix
	}
	if r.Filter.And.Prefix != nil {
		return *r.Filter.And.Prefix
	}
	return ""
}

// Tags - a rule can either have tag under <filter></filter> or under
// <filter><and></and></filter>. This method returns all the tags from the
// rule in the format tag1=value1&tag2=value2
func (r Rule) Tags() string {
	if r.Filter.Tag != nil && !r.Filter.Tag.IsEmpty() {
		return r.Filter.Tag.String()
	}
	if r.Filter.And != nil {
		if len(r.Filter.And.Tags) != 0 {
			var buf bytes.Buffer
			for _, t := range r.Filter.And.Tags {
				if buf.Len() > 0 {
					buf.WriteString("&")
				}
				buf.WriteString(t.String())
			}
			return buf.String()
		}
	}
	return ""
}

// Validate - validates the rule element
func (r Rule) Validate() error {
	if err := r.validateID(); err != nil {
		return err
	}
	if err := r.validateStatus(); err != nil {
		return err
	}
	if err := r.validateAction(); err != nil {
		return err
	}
	if err := r.validateFilter(); err != nil {
		return err
	}
	if err := r.validateNoncurrentVersion(); err != nil {
		return err
	}
	return nil
}
