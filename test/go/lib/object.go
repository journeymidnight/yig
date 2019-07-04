package lib

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype"
)

func (s3client *S3Client) PutObject(bucketName, key, value string) (err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(value)),
	}
	if _, err = s3client.Client.PutObject(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) PutObjectPreSignedWithSpecifiedBody(bucketName, key, value string, expire time.Duration) (url string, err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(value)),
	}
	req, _ := s3client.Client.PutObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) PutObjectPreSignedWithoutSpecifiedBody(bucketName, key, value string, expire time.Duration) (url string, err error) {
	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	req, _ := s3client.Client.PutObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) HeadObject(bucketName, key string) (err error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = s3client.Client.HeadObject(params)
	if err != nil {
		return err
	}
	return
}

func (s3client *S3Client) GetObject(bucketName, key string) (value string, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	out, err := s3client.Client.GetObject(params)
	if err != nil {
		return "", err
	}
	data, err := ioutil.ReadAll(out.Body)
	return string(data), err
}

func (s3client *S3Client) GetObjectOutPut(bucketName, key string) (out *s3.GetObjectOutput, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	return s3client.Client.GetObject(params)
}

func (s3client *S3Client) GetObjectPreSigned(bucketName, key string, expire time.Duration) (url string, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	req, _ := s3client.Client.GetObjectRequest(params)
	return req.Presign(expire)
}

func (s3client *S3Client) DeleteObject(bucketName, key string) (err error) {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = s3client.Client.DeleteObject(params)
	if err != nil {
		return err
	}
	return
}

func (s3client *S3Client) AppendObject(bucketName, key, value string, position int64) (nextPos int64, err error) {
	var out *s3.AppendObjectOutput
	params := &s3.AppendObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		Body:     bytes.NewReader([]byte(value)),
		Position: aws.Int64(position),
	}
	if out, err = s3client.Client.AppendObject(params); err != nil {
		return 0, err
	}

	return *out.NextPosition, nil
}

func (s3client *S3Client) PutObjectWithStorageClass(bucketName, key, value string, storageClass string) (err error) {
	params := &s3.PutObjectInput{
		Bucket:       aws.String(bucketName),
		Key:          aws.String(key),
		Body:         bytes.NewReader([]byte(value)),
		StorageClass: aws.String(storageClass),
	}
	if _, err = s3client.Client.PutObject(params); err != nil {
		return err
	}
	return
}

func (s3client *S3Client) ChangeObjectStorageClass(bucketName, key string, storageClass string) (err error) {
	params := &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(key),
		CopySource:        aws.String("/" + bucketName + "/" + key),
		MetadataDirective: aws.String("REPLACE"),
		StorageClass:      aws.String(storageClass),
	}
	if _, err = s3client.Client.CopyObject(params); err != nil {
		return err
	}
	return
}

type PostObjectInput struct {
	Url        string
	Bucket     string
	ObjName    string
	Expiration time.Time
	Date       time.Time
	Region     string
	AK         string
	SK         string
	FileSize   int
}

func (s3Client *S3Client) PostObject(pbi *PostObjectInput) error {
	commons := make(map[string]string)
	conditions := make(map[string]string)
	formParams := make(map[string]string)
	var matches [][]string
	// credential, pls refer to https://docs.amazonaws.cn/en_us/general/latest/gr/sigv4-create-string-to-sign.html
	cred := fmt.Sprintf("%s/%s/%s/s3/aws4_request", pbi.AK, pbi.Date.Format(datatype.YYYYMMDD), pbi.Region)
	encMethod := "AES256"

	commons["acl"] = "public-read"
	commons["x-amz-meta-uuid"] = "14365123651274"
	commons["x-amz-server-side-encryption"] = encMethod
	commons["x-amz-credential"] = cred
	commons["x-amz-algorithm"] = "AWS4-HMAC-SHA256"
	commons["x-amz-date"] = pbi.Date.Format(datatype.Iso8601Format)

	conditions["bucket"] = pbi.Bucket
	for k, v := range commons {
		conditions[k] = v
		formParams[k] = v
	}
	// set key
	matches = append(matches, []string{"starts-with", "$key", pbi.ObjName})

	policyStr, err := s3Client.newPostFormPolicy(pbi.Expiration, conditions, matches)
	if err != nil {
		return err
	}
	t, _ := time.Parse(datatype.Iso8601Format, commons["x-amz-date"])
	signKey := getSigningKey(pbi.SK, t, pbi.Region)
	sig := getSignature(signKey, policyStr)

	formParams["Policy"] = policyStr
	formParams["x-amz-signature"] = sig
	formParams["key"] = pbi.ObjName

	body, contentType, err := s3Client.newPostFormBody(formParams, "file", pbi.ObjName, pbi.FileSize)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", pbi.Url, body)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", contentType)
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return errors.New(fmt.Sprintf("post object for url %s returns %d", pbi.Url, resp.StatusCode))
	}

	return nil
}

type postPolicyElem struct {
	Expiration string        `json:"expiration"`
	Conditions []interface{} `json:"conditions"`
}

func (s3Client *S3Client) newPostFormPolicy(expiration time.Time, conditions map[string]string, matches [][]string) (string, error) {
	var cons []interface{}
	for k, v := range conditions {
		m := make(map[string]string)
		m[k] = v
		cons = append(cons, m)
	}
	for _, v := range matches {
		cons = append(cons, v)
	}
	ppe := &postPolicyElem{
		Expiration: expiration.Format(time.RFC3339Nano),
		Conditions: cons,
	}

	body, err := json.Marshal(ppe)
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(body)

	return encoded, nil
}

func (s3client *S3Client) newPostFormBody(params map[string]string, fieldName, fileName string, fileSize int) (*bytes.Buffer, string, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	// set the params before file segment.
	for key, val := range params {
		_ = writer.WriteField(key, val)
	}
	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		return nil, "", err
	}
	contents := RandBytes(fileSize)
	file := bytes.NewReader(contents)
	_, err = io.Copy(part, file)
	if err != nil {
		return nil, "", err
	}
	err = writer.Close()
	if err != nil {
		return nil, "", err
	}
	return body, writer.FormDataContentType(), nil
}
