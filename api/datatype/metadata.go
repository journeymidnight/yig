package datatype

import (
	"encoding/xml"
	"github.com/dustin/go-humanize"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"io"
	"io/ioutil"
	"strings"
)

const (
	MaxObjectMetaConfigurationSize = 2 * humanize.KiByte
	CustomizeMetadataHead          = "X-Amz-Meta-"
)

// supportedGetReqParams - supported request parameters for GET presigned request.
var SupportedGetReqParams = map[string]string{
	"response-expires":             "Expires",
	"response-content-type":        "Content-Type",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-language":    "Content-Language",
	"response-content-encoding":    "Content-Encoding",
}

type MetaConfiguration struct {
	XMLName   xml.Name `xml:"MetaConfiguration"`
	Xmlns     string   `xml:"Xmlns,attr,omitempty"`
	VersionID string   `xml:"VersionID,omitempty"`
	Headers   *Headers `xml:"Headers,omitempty"`
}

type Headers struct {
	XMLName  xml.Name `xml:"Headers"`
	MetaData []Meta   `xml:"MetaData,omitempty"`
}

type Meta struct {
	XMLName xml.Name `xml:"MetaData"`
	Key     string   `xml:"Key"`
	Value   string   `xml:"Value"`
}

type MetaData struct {
	VersionId string
	Data      map[string]string
}

func (w *MetaConfiguration) Validate() (metaData MetaData, error error) {
	if w == nil {
		return metaData, ErrEmptyEntity
	}

	if w.Headers.MetaData != nil {
		for _, reqHead := range w.Headers.MetaData {
			validMeta := strings.HasPrefix(reqHead.Key, CustomizeMetadataHead)
			if !validMeta {
				for _, supportHead := range SupportedGetReqParams {
					if reqHead.Key != supportHead {
						return metaData, ErrMetadataHead
					}
				}
			}
			metaData.Data[reqHead.Key] = reqHead.Value
		}
	}
	return
}

func ParseMetaConfig(reader io.Reader) (metaData MetaData, err error) {
	metaConfig := new(MetaConfiguration)
	metaBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.Logger.Error("Unable to read metadata setting body:", err)
		return metaData, err
	}
	size := len(metaBuffer)
	if size > MaxObjectMetaConfigurationSize {
		return metaData, ErrEntityTooLarge
	}
	err = xml.Unmarshal(metaBuffer, metaConfig)
	if err != nil {
		helper.Logger.Error("Unable to parse metadata setting XML body:", err)
		return metaData, ErrMalformedWebsiteConfiguration
	}
	metaData, err = metaConfig.Validate()
	if err != nil {
		return metaData, err
	}
	return metaData, nil
}
