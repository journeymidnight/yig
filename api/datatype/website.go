package datatype

import (
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"io"
	"io/ioutil"
)

const MaxBucketWebsiteRulesCount = 100

type WebsiteConfiguration struct {
	XMLName               xml.Name               `xml:"WebsiteConfiguration"`
	Xmlns                 string                 `xml:"xmlns,attr,omitempty"`
	RedirectAllRequestsTo *RedirectAllRequestsTo `xml:"RedirectAllRequestsTo,omitempty"`
	IndexDocument         *IndexDocument         `xml:"IndexDocument,omitempty"`
	ErrorDocument         *ErrorDocument         `xml:"ErrorDocument,omitempty"`
	RoutingRules          []*RoutingRule         `xml:"RoutingRules>RoutingRule,omitempty"`
}

type RedirectAllRequestsTo struct {
	XMLName  xml.Name `xml:"RedirectAllRequestsTo"`
	HostName string   `xml:"HostName"`
	Protocol string   `xml:"Protocol,omitempty"`
}

type IndexDocument struct {
	XMLName xml.Name `xml:"IndexDocument"`
	Suffix  string   `xml:"Suffix"`
}

type ErrorDocument struct {
	XMLName xml.Name `xml:"ErrorDocument"`
	Key     string   `xml:"Key"`
}

type RoutingRule struct {
	XMLName   xml.Name   `xml:"RoutingRule"`
	Condition *Condition `xml:"Condition,omitempty"`
	Redirect  *Redirect  `xml:"Redirect"`
}

type Condition struct {
	XMLName                     xml.Name `xml:"Condition"`
	KeyPrefixEquals             string   `xml:"KeyPrefixEquals,omitempty"`
	HttpErrorCodeReturnedEquals string   `xml:"HttpErrorCodeReturnedEquals,omitempty"`
}

type Redirect struct {
	XMLName              xml.Name `xml:"Redirect"`
	Protocol             string   `xml:"Protocol,omitempty"`
	HostName             string   `xml:"HostName,omitempty"`
	ReplaceKeyPrefixWith string   `xml:"ReplaceKeyPrefixWith,omitempty"`
	ReplaceKeyWith       string   `xml:"ReplaceKeyWith,omitempty"`
	HttpRedirectCode     string   `xml:"HttpRedirectCode,omitempty"`
}

func (w *WebsiteConfiguration) Validate() (valid bool) {
	if w.RedirectAllRequestsTo != nil {
		if w.IndexDocument != nil || w.ErrorDocument != nil || w.RoutingRules != nil {
			return
		}
	} else if w.IndexDocument == nil {
		return
	} else if w.RoutingRules != nil {
		if len(w.RoutingRules) == 0 || len(w.RoutingRules) > MaxBucketWebsiteRulesCount {
			return
		}
		for _, r := range w.RoutingRules {
			if r.Redirect.Protocol == "" && r.Redirect.HostName == "" && r.Redirect.ReplaceKeyPrefixWith == "" &&
				r.Redirect.ReplaceKeyWith == "" && r.Redirect.HttpRedirectCode == "" {
				return
			} else if r.Redirect.ReplaceKeyPrefixWith != "" && r.Redirect.ReplaceKeyWith != "" {
				return
			}
		}
	}
	return true
}

func ParseWebsiteConfig(reader io.Reader) (*WebsiteConfiguration, error) {
	websiteConfig := new(WebsiteConfiguration)
	websiteBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.ErrorIf(err, "Unable to read website config body")
		return nil, ErrInvalidWebsiteConfiguration
	}
	err = xml.Unmarshal(websiteBuffer, websiteConfig)
	if err != nil {
		helper.ErrorIf(err, "Unable to parse website config xml body")
		return nil, ErrMalformedWebsiteConfiguration
	}
	return websiteConfig, nil
}
