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
	CustomizeMetadataHeader        = "X-Amz-Meta-"
)

var supportedCommonMetaHeaders = []string{
	"Cache-Control",
	"Content-Disposition",
	"Content-Encoding",
	"Content-Language",
	"Content-Type",
	"Expires",
}

type MetaConfiguration struct {
	XMLName   xml.Name `xml:"MetaConfiguration"`
	Xmlns     string   `xml:"xmlns,attr,omitempty"`
	VersionID string   `xml:"VersionID,omitempty"`
	Headers   *Headers `xml:"Headers,omitempty"`
}

type Headers struct {
	XMLName  xml.Name   `xml:"Headers"`
	MetaData []MetaData `xml:"MetaData,omitempty"`
}

type MetaData struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type MetaDataReq struct {
	VersionId string
	Data      map[string]string
}

func (w *MetaConfiguration) parse() (MetaDataReq, error) {
	metaDataReq := MetaDataReq{}
	metaDataReq.Data = map[string]string{}
	if w == nil {
		return metaDataReq, ErrEmptyEntity
	}

	if w.Headers != nil && len(w.Headers.MetaData) == 0 {
		return metaDataReq, ErrEmptyEntity
	}

	if len(w.Headers.MetaData) != 0 {
		for _, reqHeader := range w.Headers.MetaData {
			customizeMeta := strings.HasPrefix(reqHeader.Key, CustomizeMetadataHeader)
			if !customizeMeta {
				for n, supportHeader := range supportedCommonMetaHeaders {
					if reqHeader.Key == supportHeader {
						break
					}
					if reqHeader.Key != supportHeader && n == len(supportedCommonMetaHeaders) {
						return metaDataReq, ErrMetadataHeader
					}
				}
			}
			metaDataReq.Data[reqHeader.Key] = reqHeader.Value
		}
	}
	return metaDataReq, nil
}

func ParseMetaConfig(reader io.Reader) (metaDataReq MetaDataReq, err error) {
	metaConfig := new(MetaConfiguration)
	metaBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.Logger.Error("Unable to read metadata setting body:", err)
		return metaDataReq, err
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
