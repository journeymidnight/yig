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

// Status represents lifecycle configuration status
type Status string

// Supported status types
const (
	Enabled  Status = "Enabled"
	Disabled Status = "Disabled"
)

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName                        xml.Name                        `xml:"Rule"`
	ID                             string                          `xml:"ID,omitempty"`
	Status                         Status                          `xml:"Status"`
	Filter                         *Filter                         `xml:"Filter,omitempty"`
	Expiration                     *Expiration                     `xml:"Expiration,omitempty"`
	Transitions                    []Transition                    `xml:"Transition,omitempty"`
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransitions   []NoncurrentVersionTransition   `xml:"NoncurrentVersionTransition,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
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
	if !r.isActionEmpty() {
		if r.AbortIncompleteMultipartUpload != nil {
			if err := r.AbortIncompleteMultipartUpload.Validate(); err != nil {
				return err
			}
			if r.Filter != nil && !r.Filter.IsTagEmpty() {
				return ErrInvalidLcTagIsNotEmpty
			}
		}

		if r.Expiration != nil {
			if err := r.Expiration.Validate(); err != nil {
				return err
			}
			if r.Expiration.IsSetExpiredObjectDeleteMarker() {
				if r.Filter != nil && !r.Filter.IsTagEmpty() {
					return ErrInvalidLcTagIsNotEmpty
				}
			}
		}

		for _, transition := range r.Transitions {
			if err := transition.Validate(); err != nil {
				return err
			}
		}

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

	return ErrLcMissingAction
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
	if r.Filter != nil {
		if r.Filter.Prefix != nil {
			return *r.Filter.Prefix
		}
		if r.Filter.And != nil && r.Filter.And.Prefix != nil {
			return *r.Filter.And.Prefix
		}
	}
	return ""
}

// Return whether rule tags are contained by object tags;
func (r Rule) filterTags(objTags map[string]string) bool {
	rTags := r.getTags()
	if len(rTags) <= len(objTags) {
		for rKey := range rTags {
			if _, ok := objTags[rKey]; ok {
				continue
			}
			return false
		}
		return true
	}
	return false
}

// Return all tags by map
func (r Rule) getTags() map[string]string {
	tags := make(map[string]string)
	if r.Filter != nil {
		if r.Filter.Tag != nil {
			tags[r.Filter.Tag.Key] = r.Filter.Tag.Value
		}
		if r.Filter.And != nil && len(r.Filter.And.Tags) != 0 {
			for _, tag := range r.Filter.And.Tags {
				tags[tag.Key] = tag.Value
			}
		}
	}

	return tags
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

	return nil
}

func (r Rule) isActionEmpty() bool {
	return r.AbortIncompleteMultipartUpload == nil && r.Expiration == nil && len(r.Transitions) == 0 &&
		r.NoncurrentVersionExpiration == nil && len(r.NoncurrentVersionTransitions) == 0
}
