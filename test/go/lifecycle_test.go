package _go

import (
	"encoding/xml"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/test/go/assert"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
	"time"
)

const (
	TestLifecycleBucket1 = "testbucket1"
	TestLifecycleBucket2 = "testbucket2"

	TestLifecycleKey1 = "noVersion/testLC"
	TestLifecycleKey2 = "version/testLC"
)

const (
	LiecycleConfigurationForNoVersion = `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>noVersion/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
							<Transition>
      								<Days>1</Days>
      								<StorageClass>` + s3.StorageClassStandardIa + `</StorageClass>
    						</Transition>
							<Transition>
      								<Days>2</Days>
      								<StorageClass>` + s3.StorageClassGlacier + `</StorageClass>
    						</Transition>
							<Expiration>
      							<Days>4</Days>
    						</Expiration>
  						</Rule>
						<Rule>
							<ID>id2</ID>
	  						<Filter>
	 								<Prefix>noVersion/</Prefix>
	  						</Filter>
	  						<Status>Enabled</Status>
							<Transition>
      								<Days>2</Days>
      								<StorageClass>` + s3.StorageClassStandardIa + `</StorageClass>
    						</Transition>
	  					</Rule>
						<Rule>
							<ID>id3</ID>
	  						<Filter>
	 								<Prefix>noVersion/</Prefix>
	  						</Filter>
	  						<Status>Enabled</Status>
							<Expiration>
      							<Days>3</Days>
    						</Expiration>
	  					</Rule>
	</LifecycleConfiguration>`

	LiecycleConfigurationForVersion = `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>version/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Days>1</Days>
      								<StorageClass>` + s3.StorageClassGlacier + `</StorageClass>
    						</Transition>
							<Expiration>
      							<Days>1</Days>
								<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
    						</Expiration>
  						</Rule>
						<Rule>
    						<ID>id2</ID>
    						<Filter>
       							<Prefix>version/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Days>2</Days>
    						</Expiration>
  						</Rule>
						<Rule>
    						<ID>id3</ID>
    						<Filter>
       							<Prefix>version/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<NoncurrentVersionTransition>
                                    <NoncurrentDays>1</NoncurrentDays>
									<StorageClass>` + s3.StorageClassStandardIa + `</StorageClass>
                            </NoncurrentVersionTransition>
  						</Rule>
						<Rule>
    						<ID>id4</ID>
    						<Filter>
       							<Prefix>version/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<NoncurrentVersionExpiration>
                                    <NoncurrentDays>2</NoncurrentDays>
							</NoncurrentVersionExpiration>
  						</Rule>
	</LifecycleConfiguration>`
)

func Test_LifecycleConfiguration(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestLifecycleBucket1)
	assert.Equal(t, err, nil, "MakeBucket err")

	// Put object, StorageClass: STANDARD
	err = sc.PutObject(TestLifecycleBucket1, TestLifecycleKey1, TEST_VALUE)
	assert.Equal(t, err, nil, "PutObject1 err")

	out, err := sc.GetObjectOutPut(TestLifecycleBucket1, TestLifecycleKey1)
	assert.Equal(t, err, nil, "GetObjectOutPut1 err")
	assert.Equal(t, *out.StorageClass == s3.StorageClassStandard, true, "object StorageClass should be STANDARD")

	// Set LC
	var config = &lifecycle.Lifecycle{}
	err = xml.Unmarshal([]byte(LiecycleConfigurationForNoVersion), config)
	assert.Equal(t, err, nil, "Unmarshal lifecycle configuration err")

	lc := TransferToS3AccessLifecycleConfiguration(config)
	assert.NotEqual(t, lc, nil, "Empty lifecycle!")

	err = sc.PutBucketLifecycle(TestLifecycleBucket1, lc)
	assert.Equal(t, err, nil, "PutBucketLifecycle err")
	t.Log("PutBucketLifecycle Success!")

	outLC, err := sc.GetBucketLifecycle(TestLifecycleBucket1)
	assert.Equal(t, err, nil, "GetBucketLifecycle err")
	t.Log("GetBucketLifecycle Success! out:", outLC)

	// Sleep wait for LC process
	time.Sleep(time.Second * 90)
	out, err = sc.GetObjectOutPut(TestLifecycleBucket1, TestLifecycleKey1)
	assert.Equal(t, err, nil, "GetObjectOutPut2 err")
	assert.Equal(t, *out.StorageClass == s3.StorageClassStandardIa, true, "object StorageClass should be STANDARD_IA")

	time.Sleep(time.Second * 60)
	_, err = sc.GetObjectOutPut(TestLifecycleBucket1, TestLifecycleKey1)
	assert.NotEqual(t, err, nil, "GetObjectOutPut3 err")
	t.Log(err)

	time.Sleep(time.Second * 210)
	_, err = sc.GetObjectOutPut(TestLifecycleBucket1, TestLifecycleKey1)
	assert.NotEqual(t, err, nil, "GetObjectOutPut4 err")
	t.Log(err)

	outLC, err = sc.DeleteBucketLifecycle(TestLifecycleBucket1)
	assert.Equal(t, err, nil, "DeleteBucketLifecycle err")
	t.Log("DeleteBucketLifecycle Success! out:", outLC)

	err = sc.DeleteBucket(TestLifecycleBucket1)
	assert.Equal(t, err, nil, "DeleteBucket err")
}

func Test_LifecycleConfigurationToVersion(t *testing.T) {
	sc := NewS3()
	var versions []string

	err := sc.MakeBucket(TestLifecycleBucket2)
	assert.Equal(t, err, nil, "MakeBucket err")

	err = sc.PutObject(TestLifecycleBucket2, TestLifecycleKey2, TEST_VALUE)
	assert.Equal(t, err, nil, "PutObject err")

	// Open bucket version
	err = sc.PutBucketVersion(TestLifecycleBucket2, s3.BucketVersioningStatusEnabled)
	assert.Equal(t, err, nil, "PutBucketVersion err")

	// Object have versionID
	putObjOut, err := sc.PutObjectOutput(TestLifecycleBucket2, TestLifecycleKey2, TEST_VALUE)
	assert.Equal(t, err, nil, "PutObject err")
	assert.NotEqual(t, putObjOut.VersionId, nil, "PutObject err")
	t.Log("VersionId:", *putObjOut.VersionId)
	versions = append(versions, *putObjOut.VersionId)

	// Add delete marker
	err = sc.DeleteObject(TestLifecycleBucket2, TestLifecycleKey2)
	assert.Equal(t, err, nil, "DeleteObject err")

	// Put object version again
	putObjOut, err = sc.PutObjectOutput(TestLifecycleBucket2, TestLifecycleKey2, TEST_VALUE)
	assert.Equal(t, err, nil, "PutObject err")
	assert.NotEqual(t, putObjOut.VersionId, nil, "PutObject err")
	t.Log("VersionId:", *putObjOut.VersionId)
	versions = append(versions, *putObjOut.VersionId)

	listOut, err := sc.ListObjects(TestLifecycleBucket2, "", "version/", 100)
	assert.Equal(t, err, nil, "ListObjects err")
	t.Log(listOut.String())

	// put lifecycle configuration
	var config = &lifecycle.Lifecycle{}
	err = xml.Unmarshal([]byte(LiecycleConfigurationForVersion), config)
	assert.Equal(t, err, nil, "Unmarshal lifecycle configuration err")

	lc := TransferToS3AccessLifecycleConfiguration(config)
	assert.NotEqual(t, lc, nil, "Empty lifecycle!")

	err = sc.PutBucketLifecycle(TestLifecycleBucket2, lc)
	assert.Equal(t, err, nil, "PutBucketLifecycle")
	outLC, err := sc.GetBucketLifecycle(TestLifecycleBucket2)
	assert.Equal(t, err, nil, "GetBucketLifecycle err")
	t.Log("GetBucketLifecycle Success! out:", outLC)

	// Sleep wait for LC process
	time.Sleep(time.Second * 90)
	_, err = sc.GetObjectVersionOutPut(TestLifecycleBucket2, TestLifecycleKey2, versions[1])
	assert.NotEqual(t, err, nil, "GetObjectVersionOutPut err")
	t.Log(err)
	getObjOut, err := sc.GetObjectVersionOutPut(TestLifecycleBucket2, TestLifecycleKey2, versions[0])
	assert.Equal(t, err, nil, "GetObjectVersionOutPut2 err")
	assert.Equal(t, *getObjOut.StorageClass == s3.StorageClassStandardIa, true, "object StorageClass should be STANDARD_IA")
	getObjOut, err = sc.GetObjectVersionOutPut(TestLifecycleBucket2, TestLifecycleKey2, "null")
	assert.Equal(t, err, nil, "GetObjectVersionOutPut3 err")
	assert.Equal(t, *getObjOut.StorageClass == s3.StorageClassStandardIa, true, "object StorageClass should be STANDARD_IA")

	time.Sleep(time.Second * 90)
	_, err = sc.GetObjectOutPut(TestLifecycleBucket2, TestLifecycleKey2)
	assert.NotEqual(t, err, nil, "GetObjectOutPut err")
	getObjOut, err = sc.GetObjectVersionOutPut(TestLifecycleBucket2, TestLifecycleKey2, versions[0])
	assert.NotEqual(t, err, nil, "GetObjectVersionOutPut4 err")
	t.Log(err)

	time.Sleep(time.Second * 90)
	listVersionOut, err := sc.ListObjectVersions(TestLifecycleBucket2, "", "", "version/", 100)
	assert.Equal(t, err, nil, "ListObjects err")
	t.Log(listVersionOut.String())
	assert.Equal(t, len(listVersionOut.Versions), 0, "Bucket should not have object")
	assert.Equal(t, len(listVersionOut.DeleteMarkers), 0, "Bucket should not have DeleteMarker")

	err = sc.DeleteBucket(TestLifecycleBucket2)
	assert.Equal(t, err, nil, "DeleteBucket err")
}
