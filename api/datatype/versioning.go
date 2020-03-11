package datatype

import (
	"encoding/xml"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

type BucketVersioningType string

const (
	BucketVersioningEnabled   BucketVersioningType = "Enabled"
	BucketVersioningDisabled  BucketVersioningType = "Disabled"
	BucketVersioningSuspended BucketVersioningType = "Suspended"
)

func (b BucketVersioningType) String() string {
	return string(b)
}

type Versioning struct {
	XMLName xml.Name             `xml:"VersioningConfiguration"`
	Status  BucketVersioningType `xml:",omitempty"`
	//TODO: MfaDelete string
}

func VersioningFromXml(xmlBytes []byte) (versioning Versioning, err error) {
	err = xml.Unmarshal(xmlBytes, &versioning)
	if err != nil {
		helper.Logger.Error("Unable to unmarshal versioning XML:", err)
		return versioning, ErrInvalidVersioning
	}
	if versioning.Status != BucketVersioningEnabled && versioning.Status != BucketVersioningSuspended {
		return versioning, ErrInvalidVersioning
	}
	return versioning, nil
}
