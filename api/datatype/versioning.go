package datatype

import (
	"encoding/xml"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
)

type Versioning struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status string
	//	MfaDelete string
}

func VersioningFromXml(xmlBytes []byte) (versioning Versioning, err error) {
	err = xml.Unmarshal(xmlBytes, &versioning)
	if err != nil {
		helper.ErrorIf(err, "Unable to unmarshal versioning XML")
		return versioning, ErrInvalidVersioningDocument
	}
	if versioning.Status != "Enabled" && versioning.Status != "Suspended" {
		return versioning, ErrInvalidVersioningDocument
	}
	return versioning, nil
}
