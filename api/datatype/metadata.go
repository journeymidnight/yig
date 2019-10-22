package datatype

import (
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig/helper"
	"io"
	"io/ioutil"
)

const (
	MaxObjectMetaConfigurationSize = 20 * humanize.KiByte
)

var supportedGetReqParams = map[string]string{
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
	VersionID string   `xml:"VersionID"`
	Headers   Headers  `xml:"Headers,omitempty"`
}

type Headers struct {
	XMLName         xml.Name        `xml:"Headers"`
	HeaderCommon    HeaderCommon    `xml:"HeaderCommon"`
	HeaderCustomize HeaderCustomize `xml:"HeaderCustomize"`
}

type HeaderCommon struct {
	XMLName  xml.Name `xml:"HeaderCommon"`
	MetaData []Meta   `xml:"MetaData,omitempty"`
}

type HeaderCustomize struct {
	XMLName  xml.Name `xml:"HeaderCustomize"`
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
	metaData.VersionId = w.VersionID
	if w.Headers.HeaderCommon.MetaData != nil {
		for _, reqHead := range w.Headers.HeaderCommon.MetaData {
			for _, supportHead := range supportedGetReqParams {
				if reqHead.Key != supportHead {
					return metaData, ErrMetaCommonHead
				}
				metaData.Data[reqHead.Key] = reqHead.Value
			}
		}
	}
	if w.Headers.HeaderCustomize.MetaData != nil {
		for _, reqHead := range w.Headers.HeaderCustomize.MetaData {
			metaData.Data[reqHead.Key] = reqHead.Value
		}
	}
	return
}

func ParseMetaConfig(reader io.Reader) (metaData MetaData, err error) {
	metaConfig := new(MetaConfiguration)
	metaBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.Logger.Error("Unable to read website config body:", err)
		return metaData, err
	}
	size := len(metaBuffer)
	if size > MaxObjectMetaConfigurationSize {
		return metaData, ErrEntityTooLarge
	}
	err = xml.Unmarshal(metaBuffer, metaConfig)
	if err != nil {
		helper.Logger.Error("Unable to parse website config XML body:", err)
		return metaData, ErrMalformedWebsiteConfiguration
	}
	metaData, err = metaConfig.Validate()
	if err != nil {
		return metaData, err
	}
	return metaData, nil
}
