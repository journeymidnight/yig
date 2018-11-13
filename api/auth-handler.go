package api

import (
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/signature"
	"net"
	"net/http"
	"regexp"
	"strings"
)

// Check request auth type verifies the incoming http request
// - validates the request signature
// - validates the policy action if anonymous tests bucket policies if any,
//   for authenticated requests validates IAM policies.
// returns APIErrorCode if any to be replied to the client.
func checkRequestAuth(api ObjectAPIHandlers, r *http.Request, action policy.Action, bucketName, objectName string) (c iam.Credential, err error) {
	// TODO:Location constraint
	switch signature.GetRequestAuthType(r) {
	case signature.AuthTypeUnknown:
		helper.Debugln("ErrAccessDenied: AuthTypeUnknown")
		return c, ErrAccessDenied
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		helper.Debugln("AuthTypeSigned")
		if c, err := signature.IsReqAuthenticated(r); err != nil {
			helper.Debugln("ErrAccessDenied: IsReqAuthenticated return false:", err)
			return c, err
		} else {
			helper.Debugln("Credential:", c)
			// check bucket policy
			return IsBucketAllowed(c, api, r, action, bucketName, objectName)
		}
	case signature.AuthTypeAnonymous:
		return IsBucketAllowed(c, api, r, action, bucketName, objectName)
	}
	return c, ErrAccessDenied
}

func IsBucketAllowed(c iam.Credential, api ObjectAPIHandlers, r *http.Request, action policy.Action, bucketName, objectName string) (iam.Credential, error) {
	bucket, err := api.ObjectAPI.GetBucket(bucketName)
	if err != nil {
		helper.Debugln("GetBucket:", err)
		return c, err
	}
	if bucket.OwnerId == c.UserId {
		return c, nil
	}
	helper.Debugln("GetBucketPolicy:", bucket.Policy)
	if bucket.Policy.IsAllowed(policy.Args{
		AccountName:     "",
		Action:          action,
		BucketName:      bucketName,
		ConditionValues: getConditionValues(r, ""),
		IsOwner:         false,
		ObjectName:      objectName,
	}) {
		c.AllowOtherUserAccess = true
		helper.Debugln("Allow", c.UserId, "access", bucketName, "with", action, objectName)
		return c, nil
	}
	helper.Debugln("ErrAccessDenied: NotAllow", c.UserId, "access", bucketName, "with", action, objectName)
	return c, ErrAccessDenied
}

func getConditionValues(request *http.Request, locationConstraint string) map[string][]string {
	args := make(map[string][]string)

	for key, values := range request.Header {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	for key, values := range request.URL.Query() {
		if existingValues, found := args[key]; found {
			args[key] = append(existingValues, values...)
		} else {
			args[key] = values
		}
	}

	args["SourceIp"] = []string{GetSourceIP(request)}

	if locationConstraint != "" {
		args["LocationConstraint"] = []string{locationConstraint}
	}

	return args
}

var (
	// De-facto standard header keys.
	xForwardedFor = http.CanonicalHeaderKey("X-Forwarded-For")
	xRealIP       = http.CanonicalHeaderKey("X-Real-IP")

	// RFC7239 defines a new "Forwarded: " header designed to replace the
	// existing use of X-Forwarded-* headers.
	// e.g. Forwarded: for=192.0.2.60;proto=https;by=203.0.113.43
	forwarded = http.CanonicalHeaderKey("Forwarded")
	// Allows for a sub-match of the first value after 'for=' to the next
	// comma, semi-colon or space. The match is case-insensitive.
	forRegex = regexp.MustCompile(`(?i)(?:for=)([^(;|,| )]+)(.*)`)
	// Allows for a sub-match for the first instance of scheme (http|https)
	// prefixed by 'proto='. The match is case-insensitive.

)

// GetSourceIP retrieves the IP from the X-Forwarded-For, X-Real-IP and RFC7239
// Forwarded headers (in that order), falls back to r.RemoteAddr when all
// else fails.
func GetSourceIP(r *http.Request) string {
	var addr string

	if fwd := r.Header.Get(xForwardedFor); fwd != "" {
		// Only grab the first (client) address. Note that '192.168.0.1,
		// 10.1.1.1' is a valid key for X-Forwarded-For where addresses after
		// the first may represent forwarding proxies earlier in the chain.
		s := strings.Index(fwd, ", ")
		if s == -1 {
			s = len(fwd)
		}
		addr = fwd[:s]
	} else if fwd := r.Header.Get(xRealIP); fwd != "" {
		// X-Real-IP should only contain one IP address (the client making the
		// request).
		addr = fwd
	} else if fwd := r.Header.Get(forwarded); fwd != "" {
		// match should contain at least two elements if the protocol was
		// specified in the Forwarded header. The first element will always be
		// the 'for=' capture, which we ignore. In the case of multiple IP
		// addresses (for=8.8.8.8, 8.8.4.4, 172.16.1.20 is valid) we only
		// extract the first, which should be the client IP.
		if match := forRegex.FindStringSubmatch(fwd); len(match) > 1 {
			// IPv6 addresses in Forwarded headers are quoted-strings. We strip
			// these quotes.
			addr = strings.Trim(match[1], `"`)
		}
	}

	if addr != "" {
		return addr
	}

	// Default to remote address if headers not set.
	addr, _, _ = net.SplitHostPort(r.RemoteAddr)
	return addr
}
