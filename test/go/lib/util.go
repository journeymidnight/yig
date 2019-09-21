package lib

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype"
	"net/url"
	"os"
	"strings"
)

func GenTestObjectUrl(sc *S3Client) string {
	return "http://" + *sc.Client.Config.Endpoint + string(os.PathSeparator) + TEST_BUCKET + string(os.PathSeparator) + TEST_KEY
}

func GenTestSpecialCharaterObjectUrl(sc *S3Client) string {
	urlchange := url.QueryEscape(TEST_KEY_SPECIAL)
	urlchange = strings.Replace(urlchange, "+", "%20", -1)
	return "http://" + *sc.Client.Config.Endpoint + string(os.PathSeparator) + TEST_BUCKET + string(os.PathSeparator) + urlchange
}

func TransferToS3AccessControlPolicy(policy *datatype.AccessControlPolicy) (s3policy *s3.AccessControlPolicy) {
	s3policy = new(s3.AccessControlPolicy)
	s3policy.Owner = new(s3.Owner)
	s3policy.Owner.ID = aws.String(policy.ID)
	s3policy.Owner.DisplayName = aws.String(policy.DisplayName)

	for _, p := range policy.AccessControlList {
		grant := new(s3.Grant)
		grant.Grantee = new(s3.Grantee)
		grant.Grantee.ID = aws.String(p.Grantee.ID)
		grant.Grantee.DisplayName = aws.String(p.Grantee.DisplayName)
		grant.Grantee.URI = aws.String(p.Grantee.URI)
		grant.Grantee.Type = aws.String(p.Grantee.XsiType)
		grant.Grantee.EmailAddress = aws.String(p.Grantee.EmailAddress)
		grant.Permission = aws.String(p.Permission)
		s3policy.Grants = append(s3policy.Grants, grant)
	}
	return
}

func (sc *S3Client) CleanEnv() {
	sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	sc.DeleteBucket(TEST_BUCKET)
}

type AccessPolicyGroup struct {
	BucketPolicy string
	BucketACL    string
	ObjectACL    string
}

type HTTPRequestToGetObjectType func(url string, requestCondition string) (status int, val []byte, err error)

func (sc *S3Client) TestAnonymousAccessResult(policyGroup AccessPolicyGroup, resultCode int) (err error) {
	err = sc.PutBucketPolicy(TEST_BUCKET, policyGroup.BucketPolicy)
	if err != nil {
		return
	}

	err = sc.PutBucketAcl(TEST_BUCKET, policyGroup.BucketACL)
	if err != nil {
		return
	}

	err = sc.PutObjectAcl(TEST_BUCKET, TEST_KEY, policyGroup.ObjectACL)
	if err != nil {
		return
	}

	status, val, err := HTTPRequestToGetObject(GenTestObjectUrl(sc))
	if status != resultCode {
		return errors.New(fmt.Sprint("Situation:", 1, "HTTPRequestToGetObject err:", err, "status:", status, "val:", val))
	}

	return nil
}

func (sc *S3Client) TestAnonymousAccessResultWithPolicyCondition(policyGroup AccessPolicyGroup, resultCode int,
	requestCondition string, HTTPRequestToGetObject HTTPRequestToGetObjectType) (err error) {
	err = sc.PutBucketAcl(TEST_BUCKET, policyGroup.BucketACL)
	if err != nil {
		return
	}

	err = sc.PutObjectAcl(TEST_BUCKET, TEST_KEY, policyGroup.ObjectACL)
	if err != nil {
		return
	}

	err = sc.PutBucketPolicy(TEST_BUCKET, policyGroup.BucketPolicy)
	if err != nil {
		return
	}

	statusCode, data, err := HTTPRequestToGetObject(GenTestObjectUrl(sc), requestCondition)
	if statusCode != resultCode {
		return errors.New(fmt.Sprint("HTTPRequestToGetObject err:", err,
			" statusCode should be ", resultCode, " but statusCode:", statusCode, " data:\n", string(data)))
	}

	return nil
}

// Generate 128KiB part data
func GenMinimalPart() []byte {
	return make([]byte, 128<<10)
}

func Format(s string) string {
	return strings.Replace(strings.Replace(strings.Replace(s, " ", "", -1), "\n", "", -1), "\t", "", -1)
}
