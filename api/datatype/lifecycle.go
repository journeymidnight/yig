package datatype

import (
	"encoding/xml"
	//	. "github.com/journeymidnight/yig/error"
	//	"github.com/journeymidnight/yig/helper"
)

type LifecycleRule struct {
	ID         string `xml:"ID"`
	Prefix     string `xml:"Prefix"`
	Status     string `xml:"Status"`
	Expiration string `xml:"Expiration>Days"`
}

type Lifecycle struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rule    []LifecycleRule `xml:"Rule"`
}
