package datatype

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

const (
	MaxBucketWebsiteRulesCount         = 100
	MaxBucketWebsiteRulesContentLength = 1 << 20
)

type WebsiteConfiguration struct {
	XMLName               xml.Name               `xml:"WebsiteConfiguration"`
	Xmlns                 string                 `xml:"xmlns,attr,omitempty"`
	RedirectAllRequestsTo *RedirectAllRequestsTo `xml:"RedirectAllRequestsTo,omitempty"`
	IndexDocument         *IndexDocument         `xml:"IndexDocument,omitempty"`
	ErrorDocument         *ErrorDocument         `xml:"ErrorDocument,omitempty"`
	RoutingRules          []RoutingRule          `xml:"RoutingRules>RoutingRule,omitempty"`
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

func (rr RoutingRule) Match(objectName string, errorCode string) bool {
	var isKeyMatch, isErrorCodeMatch = true, true
	if rr.Condition == nil {
		return true
	}
	if rr.Condition.KeyPrefixEquals != "" {
		isKeyMatch = strings.HasPrefix(objectName, rr.Condition.KeyPrefixEquals)
	}
	if rr.Condition.HttpErrorCodeReturnedEquals != "" {
		isErrorCodeMatch = (errorCode == rr.Condition.HttpErrorCodeReturnedEquals)
	}
	return isKeyMatch && isErrorCodeMatch
}

func (rr RoutingRule) DoRedirect(w http.ResponseWriter, r *http.Request, objectName string) {
	rd := rr.Redirect
	if rd == nil {
		return
	}
	protocol := rd.Protocol
	if protocol == "" {
		protocol = helper.Ternary(r.URL.Scheme == "", "http", r.URL.Scheme).(string)
	}
	hostName := rd.HostName
	if hostName == "" {
		hostName = r.Host
	}
	code := http.StatusFound
	if rd.HttpRedirectCode != "" {
		code, _ = strconv.Atoi(rd.HttpRedirectCode)
	}
	if rd.ReplaceKeyWith != "" {
		objectName = rd.ReplaceKeyWith
	} else if rd.ReplaceKeyPrefixWith != "" {
		objectName = rd.ReplaceKeyPrefixWith + strings.TrimPrefix(objectName, rr.Condition.KeyPrefixEquals)
	}
	http.Redirect(w, r, protocol+"://"+hostName+"/"+objectName, code)
	return
}

// Reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/RESTBucketPUTwebsite.html
func (w *WebsiteConfiguration) Validate() (error error) {
	if w.RedirectAllRequestsTo != nil {
		if w.IndexDocument != nil || w.ErrorDocument != nil || w.RoutingRules != nil {
			return ErrInvalidWebsiteConfiguration
		}
		protocol := w.RedirectAllRequestsTo.Protocol
		if protocol != "" && protocol != "http" && protocol != "https" {
			return ErrInvalidWebsiteRedirectProtocol
		}
	}
	if w.IndexDocument != nil {
		if w.IndexDocument.Suffix == "" || strings.Contains(w.IndexDocument.Suffix, "/") {
			return ErrInvalidIndexDocumentSuffix
		}
		if w.ErrorDocument != nil && w.ErrorDocument.Key == "" {
			return ErrInvalidErrorDocumentKey
		}
	}
	if w.RoutingRules != nil {
		if len(w.RoutingRules) == 0 {
			return ErrMissingRoutingRuleInWebsiteRules
		}
		if len(w.RoutingRules) > MaxBucketWebsiteRulesCount {
			return ErrExceededWebsiteRoutingRulesLimit
		}
		for _, r := range w.RoutingRules {
			redirect := r.Redirect
			if redirect == nil {
				return ErrMissingRedirectInWebsiteRoutingRule
			}
			if redirect.Protocol == "" && redirect.HostName == "" && redirect.ReplaceKeyPrefixWith == "" &&
				redirect.ReplaceKeyWith == "" && redirect.HttpRedirectCode == "" {
				return ErrMissingRedirectElementInWebsiteRoutingRule
			}
			if redirect.ReplaceKeyPrefixWith != "" && redirect.ReplaceKeyWith != "" {
				return ErrDuplicateKeyReplaceTagInWebsiteRoutingRule
			}
			protocol := redirect.Protocol
			if protocol != "" && protocol != "http" && protocol != "https" {
				return ErrInvalidWebsiteRedirectProtocol
			}
			if redirect.HttpRedirectCode != "" {
				code, _ := strconv.Atoi(redirect.HttpRedirectCode)
				if http.StatusText(code) == "" {
					return ErrInvalidHttpRedirectCodeInWebsiteRoutingRule
				}
			}
		}
	}
	return
}

func ParseWebsiteConfig(reader io.Reader) (*WebsiteConfiguration, error) {
	websiteConfig := new(WebsiteConfiguration)
	websiteBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.ErrorIf(err, "Unable to read website config body")
		return nil, err
	}
	size := len(websiteBuffer)
	if size > MaxBucketWebsiteRulesContentLength {
		return nil, ErrEntityTooLarge
	}
	err = xml.Unmarshal(websiteBuffer, websiteConfig)
	if err != nil {
		helper.ErrorIf(err, "Unable to parse website config xml body")
		return nil, ErrMalformedWebsiteConfiguration
	}
	err = websiteConfig.Validate()
	if err != nil {
		return nil, err
	}
	return websiteConfig, nil
}
