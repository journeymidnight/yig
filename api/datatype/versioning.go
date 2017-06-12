package datatype

import (
	"encoding/xml"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

type Versioning struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:",omitempty"`
	//TODO: MfaDelete string
}

func VersioningFromXml(xmlBytes []byte) (versioning Versioning, err error) {
	err = xml.Unmarshal(xmlBytes, &versioning)
	if err != nil {
		helper.ErrorIf(err, "Unable to unmarshal versioning XML")
		return versioning, ErrInvalidVersioning
	}
	if versioning.Status != "Enabled" && versioning.Status != "Suspended" {
		return versioning, ErrInvalidVersioning
	}
	return versioning, nil
}
