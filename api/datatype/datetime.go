package datatype

import (
	. "github.com/journeymidnight/yig/error"
	"time"
)

const (
	Iso8601Format           = "20060102T150405Z"
	YYYYMMDD                = "20060102"
	PresignedUrlExpireLimit = 7 * 24 * time.Hour
)

// Supported Amz date formats.
var amzDateFormats = []string{
	time.RFC1123,
	time.RFC1123Z,
	Iso8601Format,
	// Add new AMZ date formats here.
}

// parseAmzDate - parses date string into supported amz date formats.
func ParseAmzDate(amzDateStr string) (amzDate time.Time, apiErr error) {
	for _, dateFormat := range amzDateFormats {
		amzDate, e := time.Parse(dateFormat, amzDateStr)
		if e == nil {
			return amzDate, nil
		}
	}
	return time.Time{}, ErrMalformedDate
}
