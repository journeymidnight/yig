package datatype

import (
	"encoding/xml"
	"io/ioutil"
	"net/http"

	. "github.com/journeymidnight/yig/error"
)

type Restore struct {
	XMLName              xml.Name             `xml:"RestoreRequest"`
	Days                 int                  `xml:"Days"`
	GlacierJobParameters GlacierJobParameters `xml:"GlacierJobParameters"`
}

type GlacierJobParameters struct {
	XMLName xml.Name `xml:"GlacierJobParameters"`
	Tier    string   `xml:"Tier"`
}

func GetRestoreInfo(r *http.Request) (*Restore, error) {
	restoreInfo := &Restore{}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewError(InDatatypeFatalError, "Unable to read metadata setting body", err)
	}
	err = xml.Unmarshal(body, restoreInfo)
	if err != nil {
		return nil, ErrMalformedEncryptionConfiguration
	}

	return restoreInfo, err
}
