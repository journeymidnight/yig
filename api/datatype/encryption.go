package datatype

import (
	"encoding/xml"
	"github.com/dustin/go-humanize"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"io"
	"io/ioutil"
)

const MaxBucketEncryptionConfigurationSize = 20 * humanize.KiByte

type EncryptionConfiguration struct {
	XMLName xml.Name `xml:"ServerSideEncryptionConfiguration"`
	Rule    *Rule    `xml:"Rule,omitempty"`
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
	if e.Rule != nil {
		if e.Rule.ApplyServerSideEncryptionByDefault == nil {
			return ErrMissingEncryptionByDefaultInEncryptionRule
		}
		sseAlgorithm := e.Rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm
		masterKeyID := e.Rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID
		if sseAlgorithm == "" {
			if masterKeyID == "" {
				return ErrMissingSSEAlgorithmOrKMSMasterKeyIDInEncryptionRule
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
		helper.Logger.Error("Unable to read encryption config body:", err)
		return nil, err
	}
	size := len(encryptionBuffer)
	if size > MaxBucketEncryptionConfigurationSize {
		return nil, ErrEntityTooLarge
	}
	err = xml.Unmarshal(encryptionBuffer, encryptionConfiguration)
	if err != nil {
		helper.Logger.Error("Unable to parse encryption config XML body:", err)
		return nil, ErrMalformedEncryptionConfiguration
	}
	err = encryptionConfiguration.Validate()
	if err != nil {
		return nil, err
	}
	return encryptionConfiguration, nil
}
