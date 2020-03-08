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
	"fmt"
	"testing"
)

// appropriate errors on validation
func TestInvalidTransition(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Transition with zero days
			inputXML: ` <Transition>
                                    <Days>0</Days>
                                    </Transition>`,
			expectedErr: errLifecycleInvalidDays,
		},
		{ // Transition with invalid date
			inputXML: ` <Transition>
                                    <Date>invalid date</Date>
                                    </Transition>`,
			expectedErr: errLifecycleInvalidDate,
		},
		{ // Transition with both number of days nor a date
			inputXML: `<Transition>
		                    <Date>2019-04-20T00:01:00Z</Date>
		                    </Transition>`,
			expectedErr: errLifecycleDateNotMidnight,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var transition Transition
			err := xml.Unmarshal([]byte(tc.inputXML), &transition)
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})

	}

	validationTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Transition with a valid ISO 8601 date
			inputXML: `<Transition>
                                    <Date>2019-04-20T00:00:00Z</Date>
                                    </Transition>`,
			expectedErr: nil,
		},
		{ // Transition with a valid number of days
			inputXML: `<Transition>
                                    <Days>3</Days>
                                    </Transition>`,
			expectedErr: nil,
		},
		{ // Transition with neither number of days nor a date
			inputXML: `<Transition>
                                    </Transition>`,
			expectedErr: errLifecycleOfDateAndDays,
		},
		{ // Transition with both number of days nor a date
			inputXML: `<Transition>
                                    <Days>3</Days>
                                    <Date>2019-04-20T00:00:00Z</Date>
                                    </Transition>`,
			expectedErr: errLifecycleOfDateAndDays,
		},
	}
	for i, tc := range validationTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var transition Transition
			err := xml.Unmarshal([]byte(tc.inputXML), &transition)
			if err != nil {
				t.Fatalf("%d: %v", i+1, err)
			}

			err = transition.Validate()
			if err != tc.expectedErr {
				t.Fatalf("%d: %v", i+1, err)
			}
		})
	}
}
