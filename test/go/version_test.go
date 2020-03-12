package _go

import (
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/test/go/assert"
	. "github.com/journeymidnight/yig/test/go/lib"

	"testing"
)

func TestBucketVersioning(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteBucket(TEST_BUCKET)
	}()
	err := sc.MakeBucket(TEST_BUCKET)
	assert.Equal(t, err, nil, "MakeBucket err")
	status, err := sc.GetBucketVersion(TEST_BUCKET)
	assert.Equal(t, err, nil, "GetBucketVersion err")
	assert.Equal(t, status, "", "GetBucketVersion not empty")

	err = sc.PutBucketVersion(TEST_BUCKET, s3.BucketVersioningStatusEnabled)
	assert.Equal(t, err, nil, "PutBucketVersion err")
	status, err = sc.GetBucketVersion(TEST_BUCKET)
	assert.Equal(t, err, nil, "GetBucketVersion err")
	assert.Equal(t, status, s3.BucketVersioningStatusEnabled, "GetBucketVersion not Enabled")

	err = sc.PutBucketVersion(TEST_BUCKET, s3.BucketVersioningStatusSuspended)
	assert.Equal(t, err, nil)
	status, err = sc.GetBucketVersion(TEST_BUCKET)
	assert.Equal(t, err, nil)
	assert.Equal(t, status, s3.BucketVersioningStatusSuspended)
}

//func TestVersionedObject(t *testing.T) {
//	sc := NewS3()
//	defer func() {
//		// TODO: List and delete
//		sc.DeleteBucket(TEST_BUCKET)
//	}()
//	err := sc.MakeBucket(TEST_BUCKET)
//	assert.Equal(t, err, nil, "MakeBucket err")
//	err = sc.PutBucketVersion(TEST_BUCKET, s3.BucketVersioningStatusEnabled)
//	assert.Equal(t, err, nil, "PutBucketVersion err")
//
//	putObjOut, err := sc.PutObjectOutput(TEST_BUCKET, TEST_KEY, TEST_VALUE)
//	assert.Equal(t, err, nil, "PutObject err")
//	assert.NotEqual(t, putObjOut.VersionId, nil, "PutObject err")
//	t.Log("VersionId 1:", *putObjOut.VersionId)
//
//	out, err := sc.GetObjectOutPut(TEST_BUCKET, TEST_KEY)
//	assert.Equal(t, err, nil)
//	ver1 := *out.VersionId
//	assert.Equal(t, *putObjOut.VersionId, ver1)
//
//	// add delete marker
//	delOut, err := sc.DeleteObjectOutput(TEST_BUCKET, TEST_KEY)
//	assert.Equal(t, err, nil)
//	assert.Equal(t, delOut.DeleteMarker == nil, true)
//	assert.NotEqual(t, delOut.VersionId == nil, true)
//	ver2 := *delOut.VersionId
//	t.Log("VersionId 2:", ver2)
//
//	// if latest object is delete marker, return 404
//	out, err = sc.GetObjectOutPut(TEST_BUCKET, TEST_KEY)
//	assert.NotEqual(t, err, nil)
//
//	//  if the Latest Object Is a Delete Marker , return 404
//	out, err = sc.GetObjectVersionOutPut(TEST_BUCKET, TEST_KEY, ver2)
//	assert.NotEqual(t, err, nil)
//	assert.Equal(t, out.DeleteMarker != nil, true, "out.DeleteMarker != nil")
//	assert.Equal(t, *out.DeleteMarker, true, "")
//	assert.Equal(t, out.VersionId != nil, true, "out.VersionId != nil")
//	assert.Equal(t, *out.VersionId, ver2)
//
//	delOut, err = sc.DeleteObjectVersion(TEST_BUCKET, TEST_KEY, ver2)
//	assert.Equal(t, err, nil)
//	assert.Equal(t, delOut.DeleteMarker != nil, true, "delOut.DeleteMarker != nil")
//	assert.Equal(t, *delOut.DeleteMarker, true, "*delOut.DeleteMarker == true")
//
//	out, err = sc.GetObjectOutPut(TEST_BUCKET, TEST_KEY)
//	assert.Equal(t, err, nil)
//	assert.Equal(t, out.DeleteMarker == nil, true, "out.DeleteMarker == nil")
//	assert.Equal(t, out.VersionId != nil, true, "out.VersionId != nil")
//	assert.Equal(t, *out.VersionId, ver1, "*out.VersionId == nil", ver1)
//
//	delOut, err = sc.DeleteObjectVersion(TEST_BUCKET, TEST_KEY, ver1)
//	assert.Equal(t, err, nil)
//}
