package datatype

import (
	"encoding/xml"
	"github.com/dustin/go-humanize"
	. "github.com/journeymidnight/yig/error"
	"io"
	"io/ioutil"
)

const (
	MaxBucketEncryptionRulesCount        = 100
	MaxBucketEncryptionConfigurationSize = 20 * humanize.KiByte
)

type EncryptionConfiguration struct {
	XMLName xml.Name `xml:"ServerSideEncryptionConfiguration"`
	Rules   []*Rule  `xml:"Rule,omitempty"`
}

type Rule struct {
	XMLName                            xml.Name                            `xml:"Rule"`
	ApplyServerSideEncryptionByDefault *ApplyServerSideEncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault,omitempty"`
}

type ApplyServerSideEncryptionByDefault struct {
	XMLName        xml.Name `xml:"ApplyServerSideEncryptionByDefault"`
	KMSMasterKeyID string   `xml:"KMSMasterKeyID"`
	SSEAlgorithm   string   `xml:"SSEAlgorithm"`
}

//Reference:https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
func (e *EncryptionConfiguration) Validate() (error error) {
	if e.Rules != nil {
		if len(e.Rules) == 0 {
			return ErrMissingRuleInEncryption
		}
		if len(e.Rules) > MaxBucketEncryptionRulesCount {
			return ErrExceededEncryptionRulesLimit
		}
		for _, r := range e.Rules {
			if r.ApplyServerSideEncryptionByDefault == nil {
				return ErrMissingEncryptionByDefaultInEncryptionRule
			}
			sseAlgorithm := r.ApplyServerSideEncryptionByDefault.SSEAlgorithm
			masterKeyID := r.ApplyServerSideEncryptionByDefault.KMSMasterKeyID
			if sseAlgorithm == "" {
				if masterKeyID == "" {
					return ErrMissingSSEAlgorithmOrKMSMasterKeyIDInEncryptionRule
				}
			}
		}
	} else {
		return ErrMissingRuleInEncryption
	}

	return
}

func ParseEncryptionConfig(reader io.Reader) (*EncryptionConfiguration, error) {
	encryptionConfiguration := new(EncryptionConfiguration)
	encryptionBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, NewError(InDatatypeGeneralError, "Unable to read encryption config body", err)
	}
	size := len(encryptionBuffer)
	if size > MaxBucketEncryptionConfigurationSize {
		return nil, ErrEntityTooLarge
	}
	err = xml.Unmarshal(encryptionBuffer, encryptionConfiguration)
	if err != nil {
		return nil, ErrMalformedEncryptionConfiguration
	}
	err = encryptionConfiguration.Validate()
	if err != nil {
		return nil, err
	}
	return encryptionConfiguration, nil
}
