package iam

import (
	"fmt"
	"regexp"
)

// credential container for access and secret keys.
type Credential struct {
	UserId          string
	AccessKeyID     string
	SecretAccessKey string
}

// stringer colorized access keys.
func (a Credential) String() string {
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return fmt.Sprint(accessStr + "  " + secretStr)
}

// IsValidSecretKey - validate secret key.
var IsValidSecretKey = regexp.MustCompile(`^.{8,40}$`)

// IsValidAccessKey - validate access key.
var IsValidAccessKey = regexp.MustCompile(`^[a-zA-Z0-9\\-\\.\\_\\~]{5,20}$`)

func GetCredential(accessKey string) (credential Credential, err error) {
	// should use a cache with timeout
	// TODO
	return Credential{
		UserId:          "hehehehe",
		AccessKeyID:     accessKey,
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}
