package datatype

import (
	"encoding/xml"
//	. "legitlab.letv.cn/yig/yig/error"
//	"legitlab.letv.cn/yig/yig/helper"
)

type LcRule struct {
	ID string	`xml:"ID"`
	Prefix string	`xml:"Prefix"`
	Status string	`xml:"Status"`
	Expiration string	`xml:"Expiration>Days"`
}

type Lc struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rule []LcRule `xml:"Rule"`
}