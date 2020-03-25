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
	. "github.com/journeymidnight/yig/error"
	"testing"
)

// TestInvalidRules checks if Rule xml with invalid elements returns
// appropriate errors on validation
func TestInvalidRules(t *testing.T) {
	invalidTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // Rule without expiration action
			inputXML: ` <Rule>
                            <Status>Enabled</Status>
	                    </Rule>`,
			expectedErr: ErrLcMissingAction,
		},
		{ // Rule with ID longer than 255 characters
			inputXML: ` <Rule>
	                    <ID> babababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab </ID>
						</Rule>`,
			expectedErr: ErrInvalidLcRuleID,
		},
		{ // Rule with empty status
			inputXML: ` <Rule>
                              <Status></Status>
							  <Expiration>
									<Days>3</Days>
                                    </Expiration>
	                    </Rule>`,
			expectedErr: ErrInvalidLcRuleStatus,
		},
		{ // Rule with invalid status
			inputXML: ` <Rule>
                              <Status>OK</Status>
	                    </Rule>`,
			expectedErr: ErrInvalidLcRuleStatus,
		},
		{ // Expiration with neither number of days nor a date
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
                                    </Expiration>
	                    </Rule>`,
			expectedErr: ErrInvalidLcUsingDateAndDays,
		},
		{ // Transition with neither number of days nor a date
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
									<Days>3</Days>
                                    </Expiration>
							<Transition>
                                    
                                    </Transition>
	                    </Rule>`,
			expectedErr: ErrInvalidLcUsingDateAndDays,
		},
		{ // Transition and Expiration run success
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
									<Days>3</Days>
                                    </Expiration>
							<Transition>
                                    <Days>3</Days>
                                    </Transition>
	                    </Rule>`,
			expectedErr: ErrLcMissingStorageClass,
		},
		{ // Transition and Expiration run success
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
									<Days>3</Days>
                                    </Expiration>
							<Transition>
                                    <Days>3</Days>
                                    </Transition>
							<Transition>
                                    <Date>2019-04-20T00:00:00+08:00</Date>
                                    </Transition>
	                    </Rule>`,
			expectedErr: ErrLcMissingStorageClass,
		},
		{ // Transition and Expiration run success
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
									<Days>3</Days>
                                    </Expiration>
							<Transition>
                                    <Days>3</Days>
									<StorageClass>GLACIER</StorageClass>
                                    </Transition>
							<Transition>
                                    <Days>3</Days>
									<Date>2019-04-20T00:00:00+08:00</Date>
                                    </Transition>
	                    </Rule>`,
			expectedErr: ErrInvalidLcUsingDateAndDays,
		},
		{ // set AbortIncompleteMultipartUpload should not have tag
			inputXML: ` <Rule>
							<AbortIncompleteMultipartUpload>
         							<DaysAfterInitiation>7</DaysAfterInitiation>
      						</AbortIncompleteMultipartUpload>
						 	<Status>Enabled</Status>
							<Filter>
								<Tag>
									<Key>key1</Key>
									<Value>value1</Value>
								</Tag>
							</Filter>
	                    	<Expiration>
									<Days>3</Days>
                                    </Expiration>
							<Transition>
                                    <Days>3</Days>
									<StorageClass>GLACIER</StorageClass>
                                    </Transition>
	                    </Rule>`,
			expectedErr: ErrInvalidLcTagIsNotEmpty,
		},
		{ // set deleteMarker should not have tag
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
									<Days>3</Days>
									<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
							</Expiration>
							<Filter>
								<Tag>
									<Key>key1</Key>
									<Value>value1</Value>
								</Tag>
							</Filter>
							<Transition>
                                    <Days>3</Days>
									<StorageClass>GLACIER</StorageClass>
                                    </Transition>
							<Transition>
                                    <Days>3</Days>
                                    </Transition>
	                    </Rule>`,
			expectedErr: ErrInvalidLcTagIsNotEmpty,
		},
		{ // set deleteMarker should not have tag
			inputXML: ` <Rule>
						 	<Status>Enabled</Status>
	                    	<Expiration>
									<Days>3</Days>
									<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
							</Expiration>
							<Filter>
								<Prefix>key-prefix</Prefix>
							</Filter>
							<Transition>
                                    <Days>3</Days>
									<StorageClass>GLACIER</StorageClass>
                                    </Transition>
	                    </Rule>`,
			expectedErr: nil,
		},
	}

	for i, tc := range invalidTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var rule Rule
			err := xml.Unmarshal([]byte(tc.inputXML), &rule)
			if err != nil {
				t.Fatal(err)
			}

			if err := rule.Validate(); err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}
}
