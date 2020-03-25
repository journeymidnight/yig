package _go

import (
	"encoding/xml"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/test/go/assert"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

const (
	TestLifecycleBucket1 = "fuxian"
	TestLifecycleBucket2 = "zztet"
	TestLifecycleBucket3 = "mylifecyclebucket3"
)

const (
	LiecycleConfiguration = `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>meta/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Date>2020-03-24T00:00:00+08:00</Date>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
							<NoncurrentVersionExpiration>
                                    <NoncurrentDays>1</NoncurrentDays>
							</NoncurrentVersionExpiration>
  						</Rule>
						<Rule>
    						<ID>id2</ID>
    						<Filter>
       							<Prefix>meta/client/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Date>2020-03-24T00:00:00+08:00</Date>
    						</Expiration>
  						</Rule>
						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>meta/types/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Date>2020-03-24T00:00:00+08:00</Date>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
  						</Rule>
						<Rule>
    						<Filter>
       							<Prefix>meta/types/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Date>2020-03-24T00:00:00+08:00</Date>
    						</Expiration>
  						</Rule>
						<Rule>
    						<Filter>
       							<Prefix>meta/client/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Date>2020-03-24T00:00:00+08:00</Date>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
  						</Rule>
	</LifecycleConfiguration>`

	LiecycleConfigurationToTest = `<LifecycleConfiguration>
  						<Rule>
    						<ID>api/</ID>
							<Filter>
									<Prefix>meta/util/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Date>2020-03-24T00:00:00+08:00</Date>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
  						</Rule>
						<Rule>
    						<ID>id2</ID>
    						<Filter>
       							<Prefix>api/datatype/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Date>2020-03-24T00:00:00+08:00</Date>
    						</Expiration>
  						</Rule>
	</LifecycleConfiguration>`

	LifecycleConfigurationToVersion = `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
    						<Status>Enabled</Status>
    						<Transition>
      								<Days>1</Days>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
							<NoncurrentVersionExpiration>
                                    <NoncurrentDays>3</NoncurrentDays>
							</NoncurrentVersionExpiration>
							<NoncurrentVersionTransition>
                                    <NoncurrentDays>1</NoncurrentDays>
									<StorageClass>GLACIER</StorageClass>
                            </NoncurrentVersionTransition>
  						</Rule>
  						<Rule>
    						<ID>id2</ID>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Days>2</Days>
    						</Expiration>
  						</Rule>
	</LifecycleConfiguration>`
)

func Test_LifecycleConfiguration(t *testing.T) {
	sc := NewS3()
	//err := sc.MakeBucket(TestLifecycleBucket1)
	//if err != nil {
	//	t.Fatal("MakeBucket err:", err)
	//	panic(err)
	//}

	err := sc.PutObject(TestLifecycleBucket1, TEST_KEY, TEST_VALUE)
	assert.Equal(t, err, nil, "PutObject err")

	// open bucket version
	err = sc.PutBucketVersion(TestLifecycleBucket1, s3.BucketVersioningStatusEnabled)
	assert.Equal(t, err, nil, "PutBucketVersion err")


	var config = &lifecycle.Lifecycle{}
	err = xml.Unmarshal([]byte(LiecycleConfiguration), config)
	if err != nil {
		t.Fatal("Unmarshal lifecycle configuration err:", err)
	}

	lc := TransferToS3AccessLifecycleConfiguration(config)
	if lc == nil {
		t.Fatal("LifecycleConfiguration err:", "empty lifecycle!")
	}

	err = sc.PutBucketLifecycle(TestLifecycleBucket1, lc)
	if err != nil {
		t.Fatal("PutBucketLifecycle err:", err)
	}
	t.Log("PutBucketLifecycle Success!")

	out, err := sc.GetBucketLifecycle(TestLifecycleBucket1)
	if err != nil {
		t.Fatal("GetBucketLifecycle err:", err)
	}
	t.Log("GetBucketLifecycle Success! out:", out)

	out, err = sc.DeleteBucketLifecycle(TestLifecycleBucket2)
	if err != nil {
		t.Fatal("DeleteBucketLifecycle err:", err)
	}
	t.Log("DeleteBucketLifecycle Success! out:", out)

	//err = sc.DeleteBucket(TestLifecycleBucket1)
	//if err != nil {
	//	t.Fatal("DeleteBucket err:", err)
	//	panic(err)
	//}
}
/*
func Test_LifecycleConfigurationToVersion(t *testing.T) {
	sc := NewS3()
	var versions []string
	defer func() {
		sc.DeleteObjectVersion(TestLifecycleBucket2, TEST_KEY, "null")
		for _, version := range versions {
			sc.DeleteObjectVersion(TestLifecycleBucket2, TEST_KEY, version)
		}
		sc.DeleteBucket(TestLifecycleBucket2)
	}()

	err := sc.MakeBucket(TestLifecycleBucket2)
	assert.Equal(t, err, nil, "MakeBucket err")

	err = sc.PutObject(TestLifecycleBucket2, TEST_KEY, TEST_VALUE)
	assert.Equal(t, err, nil, "PutObject err")

	// open bucket version
	err = sc.PutBucketVersion(TestLifecycleBucket2, s3.BucketVersioningStatusEnabled)
	assert.Equal(t, err, nil, "PutBucketVersion err")

	//object have versionID
	for i := 0; i < 4; i++ {
		putObjOut, err := sc.PutObjectOutput(TestLifecycleBucket2, TEST_KEY, TEST_VALUE)
		assert.Equal(t, err, nil, "PutObject err")
		assert.NotEqual(t, putObjOut.VersionId, nil, "PutObject err")
		t.Log("VersionId", i, ":", *putObjOut.VersionId)
		versions = append(versions, *putObjOut.VersionId)
	}
	listOut, err := sc.ListObjects(TestLifecycleBucket2, "", "", 100)
	assert.Equal(t, err, nil, "ListObjects err")
	t.Log(listOut.String())


	var config = &lifecycle.Lifecycle{}
	err = xml.Unmarshal([]byte(LifecycleConfigurationToVersion), config)
	if err != nil {
		t.Fatal("Unmarshal lifecycle configuration err:", err)
	}

	lc := TransferToS3AccessLifecycleConfiguration(config)
	if lc == nil {
		t.Fatal("LifecycleConfiguration err:", "empty lifecycle!")
	}

	err = sc.PutBucketLifecycle(TestLifecycleBucket2, lc)
	if err != nil {
		t.Fatal("PutBucketLifecycle err:", err)
	}
	t.Log("PutBucketLifecycle Success!")

	time.Sleep(time.Second * 60)

	_, err = sc.GetObjectOutPut(TestLifecycleBucket2, TEST_KEY)
	assert.Equal(t, err, "NoSuchKey: The specified key does not exist", "GetObjectOutPut err")

	err = sc.DeleteBucket(TestLifecycleBucket1)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}
*/