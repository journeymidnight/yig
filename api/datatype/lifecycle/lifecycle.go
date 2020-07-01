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
	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/common"
	"strings"
	"time"
)

const (
	DebugSpec = "@every 5s"
	// DebugTime for test: 1 day == 1 second
	DebugTime = time.Second
)

const (
	// Lifecycle config can't have more than 100 rules
	RulesNumber = 100
)

// Action represents a delete action or other transition
// actions that will be implemented later.
type Action int

const (
	// NoneAction means no action required after evaluating lifecycle rules
	NoneAction Action = iota
	// DeleteAction means the object needs to be removed after evaluating lifecycle rules
	DeleteAction
	// DeleteMarker means the object deleteMarker needs to be removed after evaluating lifecycle rules
	DeleteMarkerAction
	//TransitionAction means the object storage class needs to be transitioned after evaluating lifecycle rules
	TransitionAction
	// AbortMultipartUploadAction means that abort incomplete multipart upload and delete all parts
	AbortMultipartUploadAction
)

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// IsEmpty - returns whether policy is empty or not.
func (lc Lifecycle) IsEmpty() bool {
	return len(lc.Rules) == 0
}

// ParseLifecycleConfig - parses data in given reader to Lifecycle.
func ParseLifecycleConfig(data []byte) (*Lifecycle, error) {
	lc := &Lifecycle{}
	if err := xml.Unmarshal(data, lc); err != nil {
		return nil, err
	}
	if err := lc.Validate(); err != nil {
		return nil, err
	}
	return lc, nil
}

// Validate - validates the lifecycle configuration
func (lc Lifecycle) Validate() error {
	if len(lc.Rules) > RulesNumber {
		return ErrInvalidLcRulesNumbers
	}
	// Lifecycle config should have at least one rule
	if len(lc.Rules) == 0 {
		return ErrInvalidLcRulesNumbers
	}
	// Validate all the rules in the lifecycle config
	for _, r := range lc.Rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (lc Lifecycle) FilterRulesByNonCurrentVersion() (ncvRules, cvRules, abortMultipartRules []Rule) {
	for _, rule := range lc.Rules {
		if rule.Expiration != nil || len(rule.Transitions) != 0 {
			cvRules = append(cvRules, rule)
		}
		if rule.NoncurrentVersionExpiration != nil || len(rule.NoncurrentVersionTransitions) != 0 {
			ncvRules = append(ncvRules, rule)
		}
		if rule.AbortIncompleteMultipartUpload != nil {
			abortMultipartRules = append(abortMultipartRules, rule)
		}
	}
	return
}

// ComputeAction returns the action to perform by evaluating all lifecycle rules
// against the object name and its modification time.
//
//                          The day LC run
//                                              match                  match                     No
// ----> rule ----------->prefix X objectName -------->Tags X objTags -------> IS Expiration ? -------->Select Transition from Transitions
//         ^                   | not match	 	                | not match	            | Yes                   | save/replace storageClass
//         |<-------------------                                |                       |                       | GLACIER replace STANDARD_IA
//         |<---------------------------------------------------                        |                       |
//         |                                                                       Delete object               |
//         |<---------------------------------------------------------------------------------------------------
//	FOR MANY LOOP RULES, IF NOT EXPIRATION, SHOULD BE TRANSITION(RETURN THE CHEAPEST CLASS)
//
func (lc Lifecycle) ComputeAction(objName string, objTags map[string]string, objStorageClass string, modTime time.Time,
	isExpiredObjectDeleteMarkerWork bool, rules []Rule) (Action, string) {
	var storageClass meta.StorageClass
	var action Action
	if modTime.IsZero() || objName == "" {
		return action, ""
	}

	for _, rule := range rules {
		if rule.Status == Disabled {
			continue
		}
		var prefix string
		if rule.Filter == nil {
			prefix = ""
		} else {
			prefix = rule.Prefix()
		}

		// prefix and tags pass
		if strings.HasPrefix(objName, prefix) && rule.filterTags(objTags) {
			// at the time, expiration is first,and then GLACIER
			if rule.Expiration != nil {
				if !rule.Expiration.IsDateNull() {
					if time.Now().After(rule.Expiration.Date.Time) {
						// not set EODM, only DM ==> NoneAction
						// not set EODM, only DM ==> DeleteAction
						// set EODM, only DM ==> DeleteMarkerAction
						// set EODM, not DM ==> pass Expiration
						if !rule.Expiration.IsSetExpiredObjectDeleteMarker() {
							if !isExpiredObjectDeleteMarkerWork {
								return DeleteAction, ""
							}
							return NoneAction, ""
						} else {
							if isExpiredObjectDeleteMarkerWork {
								return DeleteMarkerAction, ""
							}
						}
					}
				}

				if !rule.Expiration.IsDaysNull() {
					var days time.Duration
					if helper.CONFIG.DebugMode {
						days = time.Duration(rule.Expiration.Days) * DebugTime
					} else {
						days = time.Duration(rule.Expiration.Days) * 24 * time.Hour
					}
					if time.Now().After(modTime.Add(days)) {
						if !rule.Expiration.IsSetExpiredObjectDeleteMarker() {
							if !isExpiredObjectDeleteMarkerWork {
								return DeleteAction, ""
							}
							return NoneAction, ""
						} else {
							if isExpiredObjectDeleteMarkerWork {
								return DeleteMarkerAction, ""
							}
						}
					}
				}

			}
			if len(rule.Transitions) != 0 {
				// to result transition conflict: GLACIER > STANDARD_IA
				for _, transition := range rule.Transitions {
					if !transition.IsDateNull() {
						if time.Now().After(transition.Date.Time) {
							action = TransitionAction
							ruleStorageClass, _ := meta.MatchStorageClassIndex(transition.StorageClass)
							if storageClass < ruleStorageClass {
								storageClass = ruleStorageClass
							}
						}
					}

					if !transition.IsDaysNull() {
						var days time.Duration
						if helper.CONFIG.DebugMode {
							days = time.Duration(transition.Days) * DebugTime
						} else {
							days = time.Duration(transition.Days) * 24 * time.Hour
						}
						if time.Now().After(modTime.Add(days)) {
							action = TransitionAction
							ruleStorageClass, _ := meta.MatchStorageClassIndex(transition.StorageClass)
							if meta.StorageClassWeight[storageClass] < meta.StorageClassWeight[ruleStorageClass] {
								storageClass = ruleStorageClass
							}
						}
					}
				}
			}
		}
	}
	osc, _ := meta.MatchStorageClassIndex(objStorageClass)
	if meta.StorageClassWeight[osc] >= meta.StorageClassWeight[storageClass] {
		return NoneAction, ""
	}
	return action, storageClass.ToString()
}

// Just like ComputeAction
func (lc Lifecycle) ComputeActionForNonCurrentVersion(objName string, objTags map[string]string, objStorageClass string,
	modTime time.Time, rules []Rule) (Action, string) {
	var storageClass meta.StorageClass
	var action = NoneAction
	if modTime.IsZero() || objName == "" {
		return action, ""
	}

	for _, rule := range rules {
		var prefix string
		if rule.Filter == nil {
			prefix = ""
		} else {
			prefix = rule.Prefix()
		}

		// prefix and tags pass
		if strings.HasPrefix(objName, prefix) && rule.filterTags(objTags) {
			// at the time, expiration is first,and then GLACIER
			if rule.NoncurrentVersionExpiration != nil {
				if !rule.NoncurrentVersionExpiration.IsDaysNull() {
					var days time.Duration
					if helper.CONFIG.DebugMode {
						days = time.Duration(rule.NoncurrentVersionExpiration.NoncurrentDays) * DebugTime
					} else {
						days = time.Duration(rule.NoncurrentVersionExpiration.NoncurrentDays) * 24 * time.Hour
					}
					if time.Now().After(modTime.Add(days)) {
						return DeleteAction, ""
					}
				}

			}
			if len(rule.NoncurrentVersionTransitions) != 0 {
				// to result transition conflict: GLACIER > STANDARD_IA
				for _, transition := range rule.NoncurrentVersionTransitions {
					if !transition.IsDaysNull() {
						var days time.Duration
						if helper.CONFIG.DebugMode {
							days = time.Duration(transition.NoncurrentDays) * DebugTime
						} else {
							days = time.Duration(transition.NoncurrentDays) * 24 * time.Hour
						}
						if time.Now().After(modTime.Add(days)) {
							action = TransitionAction
							ruleStorageClass, _ := meta.MatchStorageClassIndex(transition.StorageClass)
							if meta.StorageClassWeight[storageClass] < meta.StorageClassWeight[ruleStorageClass] {
								storageClass = ruleStorageClass
							}
						}
					}

				}
			}
		}
	}
	osc, _ := meta.MatchStorageClassIndex(objStorageClass)
	if meta.StorageClassWeight[osc] >= meta.StorageClassWeight[storageClass] {
		return NoneAction, ""
	}
	return action, storageClass.ToString()
}

// ComputeAction for AbortIncompleteMultipartUpload
func (lc Lifecycle) ComputeActionForAbortIncompleteMultipartUpload(objName string, objTags map[string]string,
	modTime time.Time, rules []Rule) Action {
	var action Action
	if modTime.IsZero() || objName == "" {
		return action
	}

	for _, rule := range rules {
		if rule.Status == Disabled {
			continue
		}
		var prefix string
		if rule.Filter == nil {
			prefix = ""
		} else {
			prefix = rule.Prefix()
		}

		// prefix and tags pass
		if strings.HasPrefix(objName, prefix) && rule.filterTags(objTags) {
			if rule.AbortIncompleteMultipartUpload != nil {
				if !rule.AbortIncompleteMultipartUpload.IsDaysNull() {
					var days time.Duration
					if helper.CONFIG.DebugMode {
						days = time.Duration(rule.AbortIncompleteMultipartUpload.DaysAfterInitiation) * DebugTime
					} else {
						days = time.Duration(rule.AbortIncompleteMultipartUpload.DaysAfterInitiation) * 24 * time.Hour
					}
					if time.Now().After(modTime.Add(days)) {
						return AbortMultipartUploadAction
					}
				}
			}
		}
	}
	return action
}

// lcp finds the longest common prefix of the input strings.
// It compares by bytes instead of runes (Unicode code points).
// It's up to the caller to do Unicode normalization if desired
// (e.g. see golang.org/x/text/unicode/norm).
func Lcp(l []string) string {
	// Special cases first
	switch len(l) {
	case 0:
		return ""
	case 1:
		return l[0]
	}
	// LCP of min and max (lexigraphically)
	// is the LCP of the whole set.
	min, max := l[0], l[0]
	for _, s := range l[1:] {
		switch {
		case s < min:
			min = s
		case s > max:
			max = s
		}
	}
	for i := 0; i < len(min) && i < len(max); i++ {
		if min[i] != max[i] {
			return min[:i]
		}
	}
	// In the case where lengths are not equal but all bytes
	// are equal, min is the answer ("foo" < "foobar").
	return min
}
