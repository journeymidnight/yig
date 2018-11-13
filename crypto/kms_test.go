// Minio Cloud Storage, (C) 2015, 2016, 2017, 2018 Minio, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package crypto

import (
	"strings"
	"testing"
)

var contextWriteToTests = []struct {
	Context      Context
	ExpectedJSON string
}{
	{Context: Context{}, ExpectedJSON: "{}"},                                                    // 0
	{Context: Context{"a": "b"}, ExpectedJSON: `{"a":"b"}`},                                     // 1
	{Context: Context{"a": "b", "c": "d"}, ExpectedJSON: `{"a":"b","c":"d"}`},                   // 2
	{Context: Context{"c": "d", "a": "b"}, ExpectedJSON: `{"a":"b","c":"d"}`},                   // 3
	{Context: Context{"0": "1", "-": "2", ".": "#"}, ExpectedJSON: `{"-":"2",".":"#","0":"1"}`}, // 4
}

func TestContextWriteTo(t *testing.T) {
	for i, test := range contextWriteToTests {
		var jsonContext strings.Builder
		if _, err := test.Context.WriteTo(&jsonContext); err != nil {
			t.Errorf("Test %d: Failed to encode context: %v", i, err)
			continue
		}
		if s := jsonContext.String(); s != test.ExpectedJSON {
			t.Errorf("Test %d: JSON representation differ - got: '%s' want: '%s'", i, s, test.ExpectedJSON)
		}
	}
}
