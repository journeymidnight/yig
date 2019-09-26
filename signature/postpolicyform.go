/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package signature

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"net/http"
	"regexp"
)

var (
	// Convert to Canonical Form before compare
	EqPolicyRegExp = regexp.MustCompile("(?i)Acl|Bucket|Cache-Control|Content-Type|Content-Disposition" +
		"|Content-Encoding|Expires|Key|Success_action_redirect|Redirect|Success_action_status" +
		"|X-Amz-.+|X-Amz-Meta-.+")
	StartsWithPolicyRegExp = regexp.MustCompile("(?i)Acl|Cache-Control|Content-Type|Content-Disposition" +
		"|Content-Encoding|Expires|Key|Success_action_redirect|Redirect|X-Amz-Meta-.+")
	IgnoredFormRegExp = regexp.MustCompile("(?i)X-Amz-Signature|File|Policy|X-Ignore-.+")
)

// toString - Safely convert interface to string without causing panic.
func toString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	}
	return ""
}

// toInteger _ Safely convert interface to integer without causing panic.
func toInteger(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	}
	return 0
}

// isString - Safely check if val is of type string without causing panic.
func isString(val interface{}) bool {
	switch val.(type) {
	case string:
		return true
	}
	return false
}

// PostPolicyForm provides strict static type conversion and validation for Amazon S3's POST policy JSON string.
type PostPolicyForm struct {
	Expiration time.Time // Expiration date and time of the POST policy.
	Conditions struct {  // Conditional policy structure.
		Policies map[string]struct {
			Operator string
			Value    string
		}
		ContentLengthRange struct {
			Min int
			Max int
		}
	}
}

// parsePostPolicyForm - Parse JSON policy string into typed PostPolicyForm structure.
func parsePostPolicyForm(policy string,
	eqPolicyRegExp *regexp.Regexp, startsWithPolicyRegExp *regexp.Regexp) (PostPolicyForm, error) {
	// Convert po into interfaces and
	// perform strict type conversion using reflection.
	var rawPolicy struct {
		Expiration string        `json:"expiration"`
		Conditions []interface{} `json:"conditions"`
	}

	err := json.Unmarshal([]byte(policy), &rawPolicy)
	if err != nil {
		return PostPolicyForm{}, err
	}

	if len(rawPolicy.Conditions) == 0 {
		return PostPolicyForm{}, ErrMalformedPOSTRequest
	}

	parsedPolicy := PostPolicyForm{}

	// Parse expiry time.
	parsedPolicy.Expiration, err = time.Parse(time.RFC3339Nano, rawPolicy.Expiration)
	if err != nil {
		return PostPolicyForm{}, err
	}
	// FIXME: should be map[string][]struct{}
	parsedPolicy.Conditions.Policies = make(map[string]struct {
		Operator string
		Value    string
	})

	// Parse conditions.
	for _, val := range rawPolicy.Conditions {
		switch condt := val.(type) {
		case map[string]interface{}: // Handle key:value map types.
			for k, v := range condt {
				if !isString(v) { // Pre-check value type.
					// All values must be of type string.
					return parsedPolicy,
						fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.",
							reflect.TypeOf(condt).String(), condt)
				}
				if !eqPolicyRegExp.MatchString(k) {
					return parsedPolicy, fmt.Errorf("eq is not supported for %s", k)
				}
				// {"acl": "public-read" } is an alternate way to indicate - [ "eq", "$acl", "public-read" ]
				// In this case we will just collapse this into "eq" for all use cases.
				parsedPolicy.Conditions.Policies[http.CanonicalHeaderKey(k)] = struct {
					Operator string
					Value    string
				}{
					Operator: "eq",
					Value:    toString(v),
				}
			}
		case []interface{}: // Handle array types.
			if len(condt) != 3 { // Return error if we have insufficient elements.
				return parsedPolicy,
					fmt.Errorf("Malformed conditional fields %s of type %s found in POST policy form.",
						condt, reflect.TypeOf(condt).String())
			}
			operator := toString(condt[0])
			switch strings.ToLower(operator) {
			case "eq", "starts-with":
				for _, v := range condt { // Pre-check all values for type.
					if !isString(v) {
						// All values must be of type string.
						return parsedPolicy,
							fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.",
								reflect.TypeOf(condt).String(), condt)
					}
				}
				matchType := http.CanonicalHeaderKey(strings.TrimPrefix(toString(condt[1]), "$"))
				value := toString(condt[2])
				if operator == "eq" && !eqPolicyRegExp.MatchString(matchType) {
					return parsedPolicy, fmt.Errorf("eq is not supported for %s", matchType)
				}
				if operator == "starts-with" && !startsWithPolicyRegExp.MatchString(matchType) {
					return parsedPolicy, fmt.Errorf("starts-with is not supported for %s", matchType)
				}
				parsedPolicy.Conditions.Policies[matchType] = struct {
					Operator string
					Value    string
				}{
					Operator: operator,
					Value:    value,
				}
			case "content-length-range":
				parsedPolicy.Conditions.ContentLengthRange = struct {
					Min int
					Max int
				}{
					Min: toInteger(condt[1]),
					Max: toInteger(condt[2]),
				}
			default:
				// Condition should be valid.
				return parsedPolicy,
					fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.",
						reflect.TypeOf(condt).String(), condt)
			}
		default:
			return parsedPolicy,
				fmt.Errorf("Unknown field %s of type %s found in POST policy form.",
					condt, reflect.TypeOf(condt).String())
		}
	}
	return parsedPolicy, nil
}

// checkPostPolicy - apply policy conditions and validate input values.
func CheckPostPolicy(formValues map[string]string,
	postPolicyVersion PostPolicyType) error {

	var eqPolicyRegExp, startswithPolicyRegExp, ignoredFormRegExp *regexp.Regexp
	switch postPolicyVersion {
	case PostPolicyV2:
		eqPolicyRegExp, startswithPolicyRegExp, ignoredFormRegExp =
			EqPolicyRegExpV2, StartsWithPolicyRegExpV2, IgnoredFormRegExpV2
	case PostPolicyV4:
		eqPolicyRegExp, startswithPolicyRegExp, ignoredFormRegExp =
			EqPolicyRegExp, StartsWithPolicyRegExp, IgnoredFormRegExp
	case PostPolicyAnonymous:
		// "Requests without a security policy are considered anonymous"
		// so no need to check it
		return nil
	default:
		return ErrNotImplemented
	}
	/// Decoding policy
	policyBytes, err := base64.StdEncoding.DecodeString(formValues["Policy"])
	if err != nil {
		return ErrMalformedPOSTRequest
	}
	postPolicyForm, err := parsePostPolicyForm(string(policyBytes),
		eqPolicyRegExp, startswithPolicyRegExp)
	if err != nil {
		helper.Logger.Error("Parse post-policy form error:", err)
		return ErrMalformedPOSTRequest
	}
	if !postPolicyForm.Expiration.After(time.Now()) {
		return ErrPolicyAlreadyExpired
	}
	for name, value := range formValues {
		if ignoredFormRegExp.MatchString(name) {
			continue
		}
		if condition, ok := postPolicyForm.Conditions.Policies[name]; ok {
			switch condition.Operator {
			case "eq":
				if condition.Value != value {
					return ErrPolicyViolation
				}
			case "starts-with":
				if !strings.HasPrefix(value, condition.Value) {
					return ErrPolicyViolation
				}
			}
		} else { // field exists in form but not in policy
			// TODO make this error more specific to users
			return ErrPolicyMissingFields
		}
	}
	// TODO: verify ContentLengthRange
	return nil
}
