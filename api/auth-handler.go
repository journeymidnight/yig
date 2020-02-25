package api

import (
	"net"
	"net/http"
	"regexp"
	"strings"

	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/context"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

// Check request auth type verifies the incoming http request
// - validates the request signature
// - validates the policy action if anonymous tests bucket policies if any,
//   for authenticated requests validates IAM policies.
// returns APIErrorCode if any to be replied to the client.
func checkRequestAuth(r *http.Request, action policy.Action) (c common.Credential, err error) {
	// TODO:Location constraint
	ctx := context.GetRequestContext(r)
	logger := ctx.Logger
	authType := ctx.AuthType
	switch authType {
	case signature.AuthTypeUnknown:
		logger.Info("ErrAccessDenied: AuthTypeUnknown")
		return c, ErrSignatureVersionNotSupported
	case signature.AuthTypeSignedV4, signature.AuthTypePresignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		helper.Logger.Info("AuthTypeSigned:", authType)
		if c, err := signature.IsReqAuthenticated(r); err != nil {
			helper.Logger.Info("ErrAccessDenied: IsReqAuthenticated return false:", err)
			return c, err
		} else {
			helper.Logger.Info("Credential:", c)
			// check bucket policy
			isAllow, err := IsBucketPolicyAllowed(c.UserId, ctx.BucketInfo, r, action, ctx.ObjectName)
			c.AllowOtherUserAccess = isAllow
			return c, err
		}
	case signature.AuthTypeAnonymous:
		isAllow, err := IsBucketPolicyAllowed(c.UserId, ctx.BucketInfo, r, action, ctx.ObjectName)
		c.AllowOtherUserAccess = isAllow
		return c, err
	}
	return c, ErrAccessDenied
}

func IsBucketPolicyAllowed(userId string, bucket *meta.Bucket, r *http.Request, action policy.Action, objectName string) (allow bool, err error) {
	if bucket == nil {
		return false, ErrAccessDenied
	}
	if bucket.OwnerId == userId {
		return false, nil
	}
	policyResult := bucket.Policy.IsAllowed(policy.Args{
		// TODO: Add IAM policy. Current account name is always useless.
		AccountName:     userId,
		Action:          action,
		BucketName:      bucket.Name,
		ConditionValues: getConditionValues(r, ""),
		IsOwner:         false,
		ObjectName:      objectName,
	})
	if policyResult == policy.PolicyAllow {
		return true, nil
	} else if policyResult == policy.PolicyDeny {
		return false, ErrAccessDenied
	} else {
		return false, nil
	}

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
