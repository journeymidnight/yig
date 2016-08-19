package datatype

import (
	"encoding/xml"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

func matchOrigin(url *url.URL, allowedOrigin string) bool {
	if allowedOrigin == "*" {
		return true
	}
	allowedUrl, err := url.Parse(allowedOrigin)
	if err != nil {
		return false
	}
	if allowedUrl.Scheme == url.Scheme {
		split := strings.Split(url.Host, "*")
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

func (rule CorsRule) MatchSimple(r *http.Request) (matchedOrigin string, matched bool) {
	if !helper.StringInSlice(r.Method, rule.AllowedMethods) {
		return "", false
	}
	for _, origin := range rule.AllowedOrigins {
		if matchOrigin(r.URL, origin) {
			return origin, true
		}
	}
	return "", false
}

func (rule CorsRule) MatchPreflight(r *http.Request) (matchedOrigin string, matched bool) {
	for _, origin := range rule.AllowedOrigins {
		if matchOrigin(r.URL, origin) {
			return origin, true
		}
	}
	return "", false
}

func (rule CorsRule) SetResponseHeaders(w http.ResponseWriter, url *url.URL,
	matchedOrigin string) {
	if matchedOrigin == "*" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
	} else {
		// the CORS spec(https://www.w3.org/TR/cors) does not define
		// origin formats like "*.le.com" or "le*.com", so build a full
		// URL for response
		w.Header().Set("Access-Control-Allow-Origin",
			url.Scheme+"://"+url.Host)
	}
	if len(rule.AllowedHeaders) > 0 {
		w.Header().Set("Access-Control-Allow-Headers",
			strings.Join(rule.AllowedHeaders, ", "))
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
