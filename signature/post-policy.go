package signature

import (
	"regexp"
)

type PostPolicyType int

const (
	PostPolicyUnknown PostPolicyType = iota
	PostPolicyV2
	PostPolicyV4
	PostPolicyAnonymous
)

var (
	// Convert to Canonical Form before compare
	EqPolicyRegExpV2 = regexp.MustCompile("(?i)Acl|Bucket|Cache-Control|Content-Type|Content-Disposition" +
		"|Content-Encoding|Expires|Key|Success_action_redirect|Redirect|Success_action_status" +
		"|X-Amz-Meta-.+")
	StartsWithPolicyRegExpV2 = regexp.MustCompile("(?i)Acl|Cache-Control|Content-Type|Content-Disposition" +
		"|Content-Encoding|Expires|Key|Success_action_redirect|Redirect|X-Amz-Meta-.+")
	IgnoredFormRegExpV2 = regexp.MustCompile("(?i)Awsaccesskeyid|Signature|File|Policy|X-Ignore-.+")
)

func GetPostPolicyType(formValues map[string]string) PostPolicyType {
	if _, ok := formValues["Policy"]; !ok {
		return PostPolicyAnonymous
	}
	if _, ok := formValues["Signature"]; ok {
		return PostPolicyV2
	}
	if algorithm, ok := formValues["X-Amz-Algorithm"]; ok {
		if algorithm == SignV4Algorithm {
			return PostPolicyV4
		}
	}
	return PostPolicyUnknown
}
