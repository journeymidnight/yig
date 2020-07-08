package common

import "errors"

const {
	OSSFullAccess string = "oss_full_access"
	OSSReadOnly string = "oss_read_only"
}

// credential container for access and secret keys.
type Credential struct {
	UserId               string
	RootId               string
	DisplayName          string
	AccessKeyID          string
	SecretAccessKey      string
	OssRamPolicy         string
	AllowOtherUserAccess bool
}

func (a Credential) String() string {
	userId := "UserId: " + a.UserId
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return userId + " " + accessStr + " " + secretStr
}

func (a Credential) IsAccount() bool {
	return a.RootId == a.UserId
}

func (a Credential) IsAccountUser() bool {
	return a.RootId != a.UserId
}

func (a Credential) IsRequestAllowed() bool {
	return a.RootId != a.UserId
}

var ErrAccessKeyNotExist = errors.New("Access key does not exist")
