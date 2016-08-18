package datatype

import (
	"git.letv.cn/yig/yig/helper"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type CorsRule struct {
	Id             string
	AllowedMethods []string
	AllowedOrigins []string
	AllowedHeaders []string
	MaxAgeSeconds  int
	ExposedHeaders []string
}

func matchOrigin(url url.URL, allowedOrigin string) bool {
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

func (rule CorsRule) SetResponseHeaders(w http.ResponseWriter, url url.URL,
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
	CorsRules []CorsRule
}
