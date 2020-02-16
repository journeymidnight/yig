package datatype
import "encoding/xml"

type BucketLoggingStatus struct {
	XMLName xml.Name        `xml:"BucketLoggingStatus"`
	LoggingEnabled    BucketLoggingRule `xml:"LoggingEnabled"`
}

type BucketLoggingRule struct {
	TargetBucket     string `xml:"TargetBucket"`
	TargetPrefix     string `xml:"TargetPrefix"`
	TargetGrants     TargetGrant `xml:"TargetGrants"`
}

type TargetGrant struct {
	Grants [] BucketGrant `xml:"Grant"`
}

type BucketGrant struct {
	Grantee BucketGrantee `xml:"Grantee"`
	Permission string    `xml:"Permission"`
}
type BucketGrantee struct {
	DisplayName string `xml:"DisplayName,omitempty"`
	EmailAddress string `xml:"EmailAddress,omitempty"`
	ID string `xml:"ID,omitempty"`
	XsiType string `xml:"xsi:type,attr"`
	URI  string `xml:"URI,omitempty"`
}