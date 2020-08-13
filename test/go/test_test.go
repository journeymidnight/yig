package _go

import (
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/test/go/assert"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

func TestOpenVersioning(t *testing.T) {
	sc := NewS3()
	status, err := sc.GetBucketVersion("testversion")
	if err != nil {
		t.Error(err)
	}
	t.Log(status)

	err = sc.PutBucketVersion("testversion", s3.BucketVersioningStatusEnabled)
	assert.Equal(t, err, nil)
	status, err = sc.GetBucketVersion("testversion")
	assert.Equal(t, err, nil)
	assert.Equal(t, status, s3.BucketVersioningStatusEnabled)
}

func TestDeleteObjects(t *testing.T) {
	sc := NewS3()
	objects := make(map[string]string)
	objects["yig"] = ""
	err := sc.DeleteObjects("test", objects)
	if err != nil {
		t.Error(err)
	}
}

func TestDeleteSingleObject(t *testing.T) {
	sc := NewS3()
	result, err := sc.DeleteObjectVersion("testversion", "haha", "16849517980368749044")
	if err != nil {
		t.Error(err)
	}
	t.Log(result.VersionId)
}
