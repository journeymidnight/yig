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
	"fmt"
	. "github.com/journeymidnight/yig/error"
	"testing"
	"time"
)

func TestParseLifecycleConfig(t *testing.T) {
	// Test for  lifecycle config with more than 100 rules
	var manyRules []Rule
	rule := Rule{
		Status:     "Enabled",
		Expiration: &Expiration{Days: ExpirationDays(3)},
	}
	for i := 0; i < 101; i++ {
		manyRules = append(manyRules, rule)
	}

	manyRuleLcConfig, err := xml.Marshal(Lifecycle{Rules: manyRules})
	if err != nil {
		t.Fatal("Failed to marshal lifecycle config with more than 100 rules")
	}

	// Test for lifecycle config with rules containing overlapping prefixes
	rule1 := Rule{
		Status:     "Enabled",
		Expiration: &Expiration{Days: ExpirationDays(3)},
		Filter: &Filter{
			Prefix: String("/a/b"),
		},
	}
	rule2 := Rule{
		Status:     "Enabled",
		Expiration: &Expiration{Days: ExpirationDays(3)},
		Filter: &Filter{
			And: &And{
				Prefix: String("/a/b/c"),
			},
		},
	}
	overlappingRules := []Rule{rule1, rule2}
	overlappingLcConfig, err := xml.Marshal(Lifecycle{Rules: overlappingRules})
	if err != nil {
		t.Fatal("Failed to marshal lifecycle config with rules having overlapping prefix")
	}

	testCases := []struct {
		inputConfig string
		expectedErr error
	}{
		{ // Valid lifecycle config
			inputConfig: `<LifecycleConfiguration>
		                          <Rule>
		                          <Filter>
		                             <Prefix>prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                              <Rule>
		                          <Filter>
		                             <Prefix>another-prefix</Prefix>
		                          </Filter>
		                          <Status>Enabled</Status>
		                          <Expiration><Days>3</Days></Expiration>
		                          </Rule>
		                          </LifecycleConfiguration>`,
			expectedErr: nil,
		},
		{ // lifecycle config with no rules
			inputConfig: `<LifecycleConfiguration>
		                          </LifecycleConfiguration>`,
			expectedErr: ErrInvalidLcRulesNumbers,
		},
		{ // lifecycle config with more than 1000 rules
			inputConfig: string(manyRuleLcConfig),
			expectedErr: ErrInvalidLcRulesNumbers,
		},
		{ // lifecycle config with rules having overlapping prefix
			inputConfig: string(overlappingLcConfig),
			expectedErr: nil,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var err error
			if _, err = ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig))); err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}
}

// TestMarshalLifecycleConfig checks if lifecycleconfig xml
// marshaling/unmarshaling can handle output from each other
func TestMarshalLifecycleConfig(t *testing.T) {
	// Time at midnight Local
	midnightTS := ExpirationDate{time.Date(2019, time.April, 20, 0, 0, 0, 0, time.Local)}
	lc := Lifecycle{
		Rules: []Rule{
			{
				Status:     "Enabled",
				Filter:     &Filter{Prefix: String("prefix-1")},
				Expiration: &Expiration{Days: ExpirationDays(3)},
			},
			{
				Status:     "Enabled",
				Filter:     &Filter{Prefix: String("prefix-1")},
				Expiration: &Expiration{Date: ExpirationDate(midnightTS)},
			},
		},
	}
	b, err := xml.MarshalIndent(&lc, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	var lc1 Lifecycle
	err = xml.Unmarshal(b, &lc1)
	if err != nil {
		t.Fatal(err)
	}

	ruleSet := make(map[string]struct{})
	for _, rule := range lc.Rules {
		ruleBytes, err := xml.Marshal(rule)
		if err != nil {
			t.Fatal(err)
		}
		ruleSet[string(ruleBytes)] = struct{}{}
	}
	for _, rule := range lc1.Rules {
		ruleBytes, err := xml.Marshal(rule)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ruleSet[string(ruleBytes)]; !ok {
			t.Fatalf("Expected %v to be equal to %v, %v missing", lc, lc1, rule)
		}
	}
}

func TestComputeActions(t *testing.T) {
	testCases := []struct {
		inputConfig    string
		objectName     string
		objectTags     map[string]string
		objectModTime  time.Time
		expectedAction Action
	}{
		// 1 Empty object name (unexpected case) should always return NoneAction
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			expectedAction: NoneAction,
		},
		// 2 Disabled should always return NoneAction
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Disabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().Local().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// 3 No modTime, should be none-action
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			expectedAction: NoneAction,
		},
		// 4 Prefix not matched
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectModTime:  time.Now().Local().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// 5 Too early to remove (test Days)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectModTime:  time.Now().Local().Add(-10 * 24 * time.Hour), // Created 10 days ago
			expectedAction: NoneAction,
		},
		// 6 Should remove (test Days)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix><And></And></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().Local().Add(-6 * 24 * time.Hour), // Created 6 days ago
			expectedAction: DeleteAction,
		},
		// 7 Too early to remove (test Date)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(16*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: NoneAction,
		},
		// 8 Should remove (test Days)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// 9 Should remove (Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     map[string]string{"tag1": "value1", "tag2": "vaule2"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// 10 Should remove (Multiple Rules, Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value><Key>tag2</Key><Value>value2</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule><Rule><Filter><And><Prefix>abc/</Prefix><Tag><Key>tag2</Key><Value>value</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     map[string]string{"tag1": "value1", "tag2": "vaule2"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// 11 Should remove (Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value><Key>tag2</Key><Value>value2</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     map[string]string{"tag1": "value1", "tag2": "vaule2"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// 12 Should not remove (Tags don't match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     map[string]string{"tag1": "value1"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: NoneAction,
		},
		// 13 Should transition (Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value><Key>tag2</Key><Value>value2</Value></Tag></And></Filter><Status>Enabled</Status><Transition><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     map[string]string{"tag1": "value1", "tag2": "vaule2"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: TransitionAction,
		},
		// 13 Should remove (Tags match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value><Key>tag2</Key><Value>value2</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration><Transition><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date><StorageClass>GLACIER</StorageClass></Transition></Rule></LifecycleConfiguration>`,
			objectName:     "foodir/fooobject",
			objectTags:     map[string]string{"tag1": "value1", "tag2": "vaule2"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: DeleteAction,
		},
		// Should not remove (Tags match, but prefix doesn't match)
		{
			inputConfig:    `<LifecycleConfiguration><Rule><Filter><And><Prefix>foodir/</Prefix><Tag><Key>tag1</Key><Value>value1</Value></Tag></And></Filter><Status>Enabled</Status><Expiration><Date>` + time.Now().Truncate(24*time.Hour).Local().Add(-32*time.Hour).Format(time.RFC3339) + `</Date></Expiration></Rule></LifecycleConfiguration>`,
			objectName:     "foxdir/fooobject",
			objectTags:     map[string]string{"tag1": "value1"},
			objectModTime:  time.Now().Local().Add(-24 * time.Hour), // Created 1 day ago
			expectedAction: NoneAction,
		},
		{
			inputConfig: `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>documents/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      							<Days>30</Days>
								<StorageClass>GLACIER</StorageClass>
    						</Transition>
							<Transition>
      							<Days>60</Days>
								<StorageClass>GLACIER</StorageClass>
    						</Transition>
  						</Rule>
						</LifecycleConfiguration>`,
			objectName:     "document/document.go",
			objectModTime:  time.Now().Local().Add(-31 * 24 * time.Hour), // Created 30 day ago
			expectedAction: NoneAction,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			lc, err := ParseLifecycleConfig(bytes.NewReader([]byte(tc.inputConfig)))
			if err != nil {
				t.Fatalf("%d: Got unexpected error: %v", i+1, err)
			}
			resultAction, _ := lc.ComputeAction(tc.objectName, tc.objectTags, "", tc.objectModTime, false, lc.Rules)
			if resultAction != tc.expectedAction {
				t.Fatalf("%d: Expected action: `%v`, got: `%v`", i+1, tc.expectedAction, resultAction)
			}
		})

	}
}
