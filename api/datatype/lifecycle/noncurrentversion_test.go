package lifecycle

import (
	"encoding/xml"
	"fmt"
	. "github.com/journeymidnight/yig/error"
	"testing"
)

// appropriate errors on validation
func TestInvalidNoncurrentVersionExpiration(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // NoncurrentVersionExpiration with zero days
			inputXML: ` <NoncurrentVersionExpiration>
                                    <NoncurrentDays>0</NoncurrentDays>
                                    </NoncurrentVersionExpiration>`,
			expectedErr: ErrInvalidLcDays,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var nvExpiration NoncurrentVersionExpiration
			err := xml.Unmarshal([]byte(tc.inputXML), &nvExpiration)
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})
	}

	validationTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{
			inputXML: `<NoncurrentVersionExpiration>
                                    <NoncurrentDays>3</NoncurrentDays>
                                    </NoncurrentVersionExpiration>`,
			expectedErr: nil,
		},
		{ // NoncurrentVersionExpiration without number of days
			inputXML: `<NoncurrentVersionExpiration>
                                    </NoncurrentVersionExpiration>`,
			expectedErr: ErrLcMissingNoncurrentDays,
		},
	}
	for i, tc := range validationTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			nvExpiration := new(NoncurrentVersionExpiration)
			err := xml.Unmarshal([]byte(tc.inputXML), nvExpiration)
			if err != nil {
				t.Fatalf("%d: %v", i+1, err)
			}

			err = nvExpiration.Validate()
			if err != tc.expectedErr {
				t.Fatalf("%d: %v", i+1, err)
			}
		})
	}
}

// appropriate errors on validation
func TestInvalidNoncurrentVersionTransition(t *testing.T) {
	testCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // NoncurrentVersionExpiration with zero days
			inputXML: ` <NoncurrentVersionTransition>
                                    <NoncurrentDays>0</NoncurrentDays>
                                    </NoncurrentVersionTransition>`,
			expectedErr: ErrInvalidLcDays,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var nvTransition NoncurrentVersionTransition
			err := xml.Unmarshal([]byte(tc.inputXML), &nvTransition)
			if err != tc.expectedErr {
				t.Fatalf("%d: Expected %v but got %v", i+1, tc.expectedErr, err)
			}
		})

	}

	validationTestCases := []struct {
		inputXML    string
		expectedErr error
	}{
		{ // NoncurrentVersionTransition without StorageClass
			inputXML: `<NoncurrentVersionTransition>
                                    <NoncurrentDays>3</NoncurrentDays>
                                    </NoncurrentVersionTransition>`,
			expectedErr: ErrLcMissingStorageClass,
		},
		{ // NoncurrentVersionTransition without number of days
			inputXML: `<NoncurrentVersionTransition>
                                    </NoncurrentVersionTransition>`,
			expectedErr: ErrLcMissingNoncurrentDays,
		},
		{ // NoncurrentVersionTransition with a right format
			inputXML: `<NoncurrentVersionTransition>
									<StorageClass>GLACIER</StorageClass>
                                    </NoncurrentVersionTransition>`,
			expectedErr: ErrLcMissingNoncurrentDays,
		},
		{ // NoncurrentVersionTransition without number of days and StorageClass
			inputXML: `<NoncurrentVersionTransition>
									<NoncurrentDays>3</NoncurrentDays>
									<StorageClass>GLACIER</StorageClass>
                                    </NoncurrentVersionTransition>`,
			expectedErr: nil,
		},
	}
	for i, tc := range validationTestCases {
		t.Run(fmt.Sprintf("Test %d", i+1), func(t *testing.T) {
			var nvTransition NoncurrentVersionTransition
			err := xml.Unmarshal([]byte(tc.inputXML), &nvTransition)
			if err != nil {
				t.Fatalf("%d: %v", i+1, err)
			}

			err = nvTransition.Validate()
			if err != tc.expectedErr {
				t.Fatalf("%d: %v", i+1, err)
			}
		})
	}
}
