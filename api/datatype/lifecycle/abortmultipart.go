package lifecycle

import (
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
)

// DaysAfterInitiation is a type alias to unmarshal Days in AbortIncompleteMultipartUpload
type DaysAfterInitiation int

// UnmarshalXML parses number of days and validates if greater than zero
func (aDays *DaysAfterInitiation) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var numDays int
	err := d.DecodeElement(&numDays, &startElement)
	if err != nil {
		return NewError(InDatatypeFatalError, "Unmarshal err", err)
	}
	if numDays <= 0 {
		return ErrInvalidLcDays
	}
	*aDays = DaysAfterInitiation(numDays)
	return nil
}

// MarshalXML encodes number of days to expire if it is non-zero and
// encodes empty string otherwise
func (aDays *DaysAfterInitiation) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if *aDays == DaysAfterInitiation(0) {
		return nil
	}
	return e.EncodeElement(int(*aDays), startElement)
}

// AbortIncompleteMultipartUpload - an action for lifecycle configuration rule.
type AbortIncompleteMultipartUpload struct {
	XMLName             xml.Name            `xml:"AbortIncompleteMultipartUpload,omitempty"`
	DaysAfterInitiation DaysAfterInitiation `xml:"DaysAfterInitiation,omitempty"`
}

// Validate - validates the "AbortIncompleteMultipartUpload" element
func (n AbortIncompleteMultipartUpload) Validate() error {
	if n.IsDaysNull() {
		return ErrLcMissingDaysAfterInitiation
	}

	return nil
}

// IsDaysNull returns true if days field is null
func (n AbortIncompleteMultipartUpload) IsDaysNull() bool {
	return n.DaysAfterInitiation == DaysAfterInitiation(0)
}
