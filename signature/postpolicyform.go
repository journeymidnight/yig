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
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

// startWithConds - map which indicates if a given condition supports starts-with policy operator
var startsWithConds = map[string]bool{
	"$acl":                     true,
	"$bucket":                  false,
	"$cache-control":           true,
	"$content-type":            true,
	"$content-disposition":     true,
	"$content-encoding":        true,
	"$expires":                 true,
	"$key":                     true,
	"$success_action_redirect": true,
	"$redirect":                true,
	"$success_action_status":   false,
	"$x-amz-algorithm":         false,
	"$x-amz-credential":        false,
	"$x-amz-date":              false,
}

// Add policy conditionals.
const (
	policyCondEqual         = "eq"
	policyCondStartsWith    = "starts-with"
	policyCondContentLength = "content-length-range"
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
func toInteger(val interface{}) (int64, error) {
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		i, err := strconv.Atoi(v)
		return int64(i), err
	default:
		return 0, errors.New("Invalid number format")
	}
}

// isString - Safely check if val is of type string without causing panic.
func isString(val interface{}) bool {
	switch val.(type) {
	case string:
		return true
	}
	return false
}

// toLowerString - safely convert interface to lower string
func toLowerString(val interface{}) string {
	return strings.ToLower(toString(val))
}

// ContentLengthRange - policy content-length-range field.
type contentLengthRange struct {
	Min   int64
	Max   int64
	Valid bool // If content-length-range was part of policy
}

// PostPolicyForm provides strict static type conversion and validation for Amazon S3's POST policy JSON string.
type PostPolicyForm struct {
	Expiration time.Time // Expiration date and time of the POST policy.
	Conditions struct {
		// Conditional policy structure.
		Policies []struct {
			Operator string
			Key      string
			Value    string
		}
		ContentLengthRange contentLengthRange
	}
}

// parsePostPolicyForm - Parse JSON policy string into typed PostPolicyForm structure.
func parsePostPolicyForm(policy string) (PostPolicyForm, error) {
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

	parsedPolicy := PostPolicyForm{}

	// Parse expiry time.
	parsedPolicy.Expiration, err = time.Parse(time.RFC3339Nano, rawPolicy.Expiration)
	if err != nil {
		return PostPolicyForm{}, err
	}

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

				// {"acl": "public-read" } is an alternate way to indicate - [ "eq", "$acl", "public-read" ]
				// In this case we will just collapse this into "eq" for all use cases.
				parsedPolicy.Conditions.Policies = append(parsedPolicy.Conditions.Policies, struct {
					Operator string
					Key      string
					Value    string
				}{
					policyCondEqual, "$" + toLowerString(k), toString(v),
				})
			}
		case []interface{}: // Handle array types.
			if len(condt) != 3 { // Return error if we have insufficient elements.
				return parsedPolicy,
					fmt.Errorf("Malformed conditional fields %s of type %s found in POST policy form.",
						condt, reflect.TypeOf(condt).String())
			}
			switch toLowerString(condt[0]) {
			case policyCondEqual, policyCondStartsWith:
				for _, v := range condt { // Pre-check all values for type.
					if !isString(v) {
						// All values must be of type string.
						return parsedPolicy,
							fmt.Errorf("Unknown type %s of conditional field value %s found in POST policy form.",
								reflect.TypeOf(condt).String(), condt)
					}
				}
				operator, matchType, value := toLowerString(condt[0]), toLowerString(condt[1]), toString(condt[2])
				if !strings.HasPrefix(matchType, "$") {
					return parsedPolicy, fmt.Errorf("Invalid according to Policy: Policy Condition failed: [%s, %s, %s]", operator, matchType, value)
				}
				parsedPolicy.Conditions.Policies = append(parsedPolicy.Conditions.Policies, struct {
					Operator string
					Key      string
					Value    string
				}{
					operator, matchType, value,
				})
			case policyCondContentLength:
				min, err := toInteger(condt[1])
				if err != nil {
					return parsedPolicy, err
				}

				max, err := toInteger(condt[2])
				if err != nil {
					return parsedPolicy, err
				}

				parsedPolicy.Conditions.ContentLengthRange = contentLengthRange{
					Min:   min,
					Max:   max,
					Valid: true,
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

// checkPolicyCond returns a boolean to indicate if a condition is satisified according
// to the passed operator
func checkPolicyCond(op string, input1, input2 string) bool {
	switch op {
	case policyCondEqual:
		return input1 == input2
	case policyCondStartsWith:
		return strings.HasPrefix(input1, input2)
	}
	return false
}

// checkPostPolicy - apply policy conditions and validate input values.
func CheckPostPolicy(formValues map[string]string) error {
	/// Decoding policy
	policyBytes, err := base64.StdEncoding.DecodeString(formValues["Policy"])
	if err != nil {
		return ErrMalformedPOSTRequest
	}
	postPolicyForm, err := parsePostPolicyForm(string(policyBytes))
	if err != nil {
		helper.Logger.Error("Parse post-policy form error:", err)
		return ErrMalformedPOSTRequest
	}
	if !postPolicyForm.Expiration.After(time.Now()) {
		return ErrPolicyAlreadyExpired
	}

	// Flag to indicate if all policies conditions are satisfied
	var condPassed bool

	// Iterate over policy conditions and check them against received form fields
	for _, policy := range postPolicyForm.Conditions.Policies {
		// Form fields names are in canonical format, convert conditions names
		// to canonical for simplification purpose, so `$key` will become `Key`
		formCanonicalName := http.CanonicalHeaderKey(strings.TrimPrefix(policy.Key, "$"))
		// Operator for the current policy condition
		op := policy.Operator
		// If the current policy condition is known
		if startsWithSupported, condFound := startsWithConds[policy.Key]; condFound {
			// Check if the current condition supports starts-with operator
			if op == policyCondStartsWith && !startsWithSupported {
				return fmt.Errorf("Invalid according to Policy: Policy Condition failed")
			}
			// Check if current policy condition is satisfied
			condPassed = checkPolicyCond(op, formValues[formCanonicalName], policy.Value)
			if !condPassed {
				return fmt.Errorf("Invalid according to Policy: Policy Condition failed")
			}
		} else {
			// This covers all conditions X-Amz-Meta-* and X-Amz-*
			if strings.HasPrefix(policy.Key, "$x-amz-meta-") || strings.HasPrefix(policy.Key, "$x-amz-") {
				// Check if policy condition is satisfied
				condPassed = checkPolicyCond(op, formValues[formCanonicalName], policy.Value)
				if !condPassed {
					return fmt.Errorf("Invalid according to Policy: Policy Condition failed: [%s, %s, %s]", op, policy.Key, policy.Value)
				}
			} else {
				// check custom form fields
				condPassed = checkPolicyCond(op, formValues[formCanonicalName], policy.Value)
				if !condPassed {
					return fmt.Errorf("Invalid according to Policy: Policy Condition failed: [%s, %s, %s]", op, policy.Key, policy.Value)
				}
			}
		}
	}
	// TODO: Check Content Range
	return nil
}
