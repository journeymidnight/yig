package datatype

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

const (
	MAX_CORS_SIZE = 64 << 10 // 64 KB
)

type CorsRule struct {
	Id             string   `xml:"ID"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedHeaders []string `xml:"AllowedHeader"`
	MaxAgeSeconds  int
	ExposedHeaders []string `xml:"ExposeHeader"`
}

func matchOrigin(urlStr string, allowedOrigin string) bool {
	if allowedOrigin == "*" {
		return true
	}
	allowedUrl, err := url.Parse(allowedOrigin)
	if err != nil {
		return false
	}
	url, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	if allowedUrl.Scheme == url.Scheme ||
		(allowedUrl.Scheme == "" && url.Scheme == "http") {
		split := strings.Split(allowedUrl.Host, "*")
		if len(split) == 1 { // no "*" in allowed origin
			if url.Host == allowedUrl.Host {
				return true
			}
		} else if len(split) == 2 { // one "*" in allowed origin
			if strings.HasPrefix(url.Host, split[0]) &&
				strings.HasSuffix(url.Host, split[1]) {
				return true
			}
		}
	}
	return false
}

func (rule CorsRule) MatchSimple(r *http.Request) (matched bool) {
	if !helper.StringInSlice(r.Method, rule.AllowedMethods) {
		return false
	}
	for _, origin := range rule.AllowedOrigins {
		if matchOrigin(r.Header.Get("Origin"), origin) {
			return true
		}
	}
	return false
}

func (rule CorsRule) MatchPreflight(r *http.Request) (matched bool) {
	if !helper.StringInSlice(r.Header.Get("Access-Control-Request-Method"), rule.AllowedMethods) {
		return false
	}
	for _, origin := range rule.AllowedOrigins {
		if matchOrigin(r.Header.Get("Origin"), origin) {
			return true
		}
	}
	return false
}

func (rule CorsRule) SetResponseHeaders(w http.ResponseWriter, r *http.Request, origin string) {
	if origin != "" {
		// the CORS spec(https://www.w3.org/TR/cors) does not define
		// origin formats like "*.le.com" or "le*.com", so build a full
		// URL for response
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	if len(rule.AllowedHeaders) > 0 {
		if len(rule.AllowedHeaders) == 1 && rule.AllowedHeaders[0] == "*" {
			requestHeaders, ok := r.Header["Access-Control-Request-Headers"]
			if !ok {
				w.Header().Set("Access-Control-Allow-Headers", "*")
			} else {
				for _, header := range requestHeaders {
					w.Header().Add("Access-Control-Allow-Headers", header)
				}
			}
		} else {
			for _, header := range rule.AllowedHeaders {
				w.Header().Add("Access-Control-Allow-Headers", header)
			}
		}
	}
	if len(rule.AllowedMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods",
			strings.Join(rule.AllowedMethods, ", "))
	}
	if len(rule.ExposedHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers",
			strings.Join(rule.ExposedHeaders, ", "))
	}
	if rule.MaxAgeSeconds > 0 {
		w.Header().Set("Access-Control-Max-Age",
			strconv.Itoa(rule.MaxAgeSeconds))
	}
}

type Cors struct {
	XMLName   xml.Name   `xml:"CORSConfiguration" json:"-"`
	CorsRules []CorsRule `xml:"CORSRule"`
}

func CorsFromXml(corsBuffer []byte) (cors Cors, err error) {
	helper.Debugln("Incoming CORS XML:", string(corsBuffer))
	err = xml.Unmarshal(corsBuffer, &cors)
	if err != nil {
		helper.ErrorIf(err, "Unable to unmarshal CORS XML")
		return cors, ErrInvalidCorsDocument
	}
	if len(cors.CorsRules) == 0 {
		return cors, ErrInvalidCorsDocument
	}
	for _, rule := range cors.CorsRules {
		if len(rule.AllowedMethods) == 0 || len(rule.AllowedOrigins) == 0 {
			return cors, ErrInvalidCorsDocument
		}
	}
	return cors, nil
}
