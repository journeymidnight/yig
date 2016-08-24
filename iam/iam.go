// +build !debug

package iam

import (
	"fmt"
	"github.com/chasex/redis-go-cluster"
	"regexp"
	"time"
)

// credential container for access and secret keys.
type Credential struct {
	UserId          string
	DisplayName     string
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
	// TODO put redis addr to config

	cluster, err := redis.NewCluster(
		&redis.Options{
			StartNodes:   []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"},
			ConnTimeout:  50 * time.Millisecond,
			ReadTimeout:  50 * time.Millisecond,
			WriteTimeout: 50 * time.Millisecond,
			KeepAlive:    16,
			AliveTime:    60 * time.Second,
		})
	if err != nil {
		return Credential{}, fmt.Errorf("connect to redis failed")
	}
	reply, err := redis.Strings(cluster.Do("HMGET", accessKey, "secretKey", "uid"))
	if err != nil {
		return Credential{}, fmt.Errorf("HMGET failed")
	}

	return Credential{
		UserId:          reply[1],
		DisplayName:     reply[1],
		AccessKeyID:     accessKey,
		SecretAccessKey: reply[0],
	}, nil // For test now
}

func GetCredentialByUserId(userId string) (credential Credential, err error) {
	// should use a cache with timeout
	// TODO
	return Credential{
		UserId:          userId,
		DisplayName:     userId,
		AccessKeyID:     "hehehehe",
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}
