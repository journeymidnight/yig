package common

import (
	"github.com/journeymidnight/yig/api/datatype/policy"
)

const (
	OSSFullAccess = "oss_full_access"
	OSSReadOnly   = "oss_read_only"
)

// credential container for access and secret keys.
type Credential struct {
	// UserId               string
	ExternUserId         string
	ExternRootId         string
	ExternRootName       string
	DisplayName          string
	AccessKeyID          string
	SecretAccessKey      string
	Policy               *policy.Policy
	AllowOtherUserAccess bool
}

func (a Credential) String() string {
	// userId := "UserId: " + a.UserId
	externUserId := "ExternUserId: " + a.ExternUserId
	externRootId := "ExternRootId: " + a.ExternRootId
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return externUserId + " " + externRootId + " " + accessStr + " " + secretStr
}
