package datatype

type BucketLoggingStatus struct {
	LoggingEnabled BucketLoggingRule `xml:"LoggingEnabled"`
	SetTime        string
	SetLog         bool
	DeleteTime     string
}

type BucketLoggingRule struct {
	TargetBucket string `xml:"TargetBucket"`
	TargetPrefix string `xml:"TargetPrefix"`
}
