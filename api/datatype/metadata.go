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

var supportedCommonMetaHeaders = []string{
	"cache-control",
	"content-disposition",
	"content-encoding",
	"content-language",
	"content-type",
	"expires",
}

type MetaConfiguration struct {
	XMLName   xml.Name `xml:"MetaConfiguration"`
	Xmlns     string   `xml:"Xmlns,attr,omitempty"`
	VersionID string   `xml:"VersionID,omitempty"`
	Headers   *Headers `xml:"Headers,omitempty"`
}

type Headers struct {
	XMLName  xml.Name `xml:"Headers"`
	MetaData []MetaData   `xml:"MetaData,omitempty"`
}

type MetaData struct {
	XMLName xml.Name `xml:"MetaData"`
	Key     string   `xml:"Key"`
	Value   string   `xml:"Value"`
}

type MetaDataReq struct {
	VersionId string
	Data      map[string]string
}

func (w *MetaConfiguration) parse() (metaData MetaDataReq, error error) {
	if w == nil {
		return metaData, ErrEmptyEntity
	}

	if w.Headers == nil {
		return metaData, ErrEmptyEntity
	}

	if len(w.Headers.MetaData) != 0 {
		for _, reqHeader := range w.Headers.MetaData {
			validMeta := strings.HasPrefix(reqHeader.Key, CustomizeMetadataHead)
			if !validMeta {
				for _, supportHeader := range supportedCommonMetaHeaders {
					if reqHeader.Key != supportHeader {
						return metaData, ErrMetadataHeader
					}
				}
			}
			metaData.Data[reqHeader.Key] = reqHeader.Value
		}
	}
	return
}

func ParseMetaConfig(reader io.Reader) (metaDataReq MetaDataReq, err error) {
	metaConfig := new(MetaConfiguration)
	metaBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.Logger.Error("Unable to read metadata setting body:", err)
		return metaDataReq, err
	}
	size := len(metaBuffer)
	if size > MaxObjectMetaConfigurationSize {
		return metaDataReq, ErrEntityTooLarge
	}
	err = xml.Unmarshal(metaBuffer, metaConfig)
	if err != nil {
		helper.Logger.Error("Unable to parse metadata XML body:", err)
		return metaDataReq, ErrMalformedMetadataConfiguration
	}
	metaDataReq, err = metaConfig.parse()
	if err != nil {
		return metaDataReq, err
	}
	return metaDataReq, nil
}
