package sts

import (
	"fmt"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
)

const (
	SecurityTokenHeader = "X-Amz-Security-Token"

	expirationFormat = "2006-01-02T15:04:05.000Z"
)

func VerifyToken(accessKey string, token string) (common.Credential, error) {
	federationToken, err := UnpackV1([]byte(helper.CONFIG.StsEncryptionKey), token)
	if err != nil {
		return common.Credential{}, ErrInvalidAccessKeyID
	}
	if federationToken.AccessKey != accessKey {
		return common.Credential{}, ErrInvalidAccessKeyID
	}
	expireTime, err := time.Parse(expirationFormat, federationToken.Expiration)
	if err != nil {
		return common.Credential{}, ErrInvalidAccessKeyID
	}
	if time.Now().UTC().After(expireTime) {
		return common.Credential{}, ErrExpiredPresignRequest
	}

	return common.Credential{
		ExternUserId: federationToken.OriginalUser,
		ExternRootId: federationToken.OriginalUser,
		DisplayName: fmt.Sprintf("%s:%s",
			federationToken.OriginalUser, federationToken.Name),
		AccessKeyID:     accessKey,
		SecretAccessKey: federationToken.SecretKey,
	}, nil
}
