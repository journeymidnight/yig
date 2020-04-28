package datatype

type BucketLoggingStatus struct {
	LoggingEnabled BucketLoggingRule `xml:"LoggingEnabled"`
}

type BucketLoggingRule struct {
	TargetBucket string `xml:"TargetBucket"`
	TargetPrefix string `xml:"TargetPrefix"`
}
