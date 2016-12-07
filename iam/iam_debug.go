// +build debug

package iam

import (
	"regexp"
)

// IsValidSecretKey - validate secret key.
var IsValidSecretKey = regexp.MustCompile(`^.{8,40}$`)

// IsValidAccessKey - validate access key.
var IsValidAccessKey = regexp.MustCompile(`^[a-zA-Z0-9\\-\\.\\_\\~]{5,20}$`)

func GetCredential(accessKey string) (credential Credential, err error) {
	// should use a cache with timeout
	// TODO
	return Credential{
		UserId:          "hehehehe",
		DisplayName:     "hehehehe",
		AccessKeyID:     accessKey,
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}

func GetCredentialByUserId(userId string) (credential Credential, err error) {
	// should use a cache with timeout
	// TODO
	return Credential{
		UserId:          userId,
		DisplayName:     "hehehehe",
		AccessKeyID:     "hehehehe",
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}
