package _go

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

type S3Client struct {
	Client *s3.S3
}

func NewS3() *S3Client {
	c, err := ReadConfig()
	if err != nil {
		panic("New S3 err:" + err.Error())
	}

	return &S3Client{SessionNew(c)}
}

func NewS3ByConf(c *Config) *S3Client {
	return &S3Client{SessionNew(c)}
}

func ReadConfig() (*Config, error) {
	f, err := os.Open("/etc/yig/yig.json")
	if err != nil {
		return nil, errors.New("Cannot open /etc/yig/yig.json")
	}
	defer f.Close()

	conf := GetDefaultConfigPath()
	c, err := loadConfigFile(conf)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	return c, nil
}
