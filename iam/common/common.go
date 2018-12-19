package common

import "errors"

// credential container for access and secret keys.
type Credential struct {
	UserId               string
	DisplayName          string
	AccessKeyID          string
	SecretAccessKey      string
	AllowOtherUserAccess bool
}

func (a Credential) String() string {
	userId := "UserId: " + a.UserId
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return userId + " " + accessStr + " " + secretStr + "\n"
}

var ErrAccessKeyNotExist = errors.New("Access key does not exist")
