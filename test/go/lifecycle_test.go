package _go

import (
	"encoding/xml"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
)

const (
	TestLifecycleBucket1 = "mylifecyclebucket1"
	TestLifecycleBucket2 = "mylifecyclebucket2"
	TestLifecycleBucket3 = "mylifecyclebucket3"
)

const (
	LiecycleConfiguration = `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>documents/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Days>30</Days>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
							<NoncurrentVersionTransition>
                                    <NoncurrentDays>3</NoncurrentDays>
									<StorageClass>GLACIER</StorageClass>
                            </NoncurrentVersionTransition>
  						</Rule>
  						<Rule>
    						<ID>id2</ID>
    						<Filter>
       							<Prefix>logs/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Days>365</Days>
    						</Expiration>
							<NoncurrentVersionExpiration>
                                    <NoncurrentDays>3</NoncurrentDays>
							</NoncurrentVersionExpiration>
  						</Rule>
						<Rule>
    						<Filter>
       							<Prefix>test/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Days>365</Days>
    						</Expiration>
  						</Rule>
	</LifecycleConfiguration>`

	LifecycleWrongConfiguration = `<LifecycleConfiguration>
  						<Rule>
    						<ID>id1</ID>
							<Filter>
									<Prefix>documents/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Transition>
      								<Days>30</Days>
      								<StorageClass>GLACIER</StorageClass>
    						</Transition>
  						</Rule>
  						<Rule>
    						<ID>id2</ID>
    						<Filter>
       							<Prefix>logs/</Prefix>
    						</Filter>
    						<Status>Enabled</Status>
    						<Expiration>
      							<Days>365</Days>
    						</Expiration>
  						</Rule>
	</LifecycleConfiguration>`
)

func Test_RightLifecycleConfiguration(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestLifecycleBucket1)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	var config = &lifecycle.Lifecycle{}
	err = xml.Unmarshal([]byte(LifecycleWrongConfiguration), config)
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

	out, err = sc.DeleteBucketLifecycle(TestLifecycleBucket1)
	if err != nil {
		t.Fatal("DeleteBucketLifecycle err:", err)
	}
	t.Log("DeleteBucketLifecycle Success! out:", out)

	err = sc.DeleteBucket(TestLifecycleBucket1)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
}


//func Test_LifeCycle(t *testing.T) {
//	sc := NewS3()
//
//	//Create bucket.
//	err := sc.MakeBucket(TEST_BUCKET)
//	if err != nil {
//		t.Fatal("MakeBucket err:", err)
//	}
//	t.Log("CreateBucket Success!")
//
//	//Put object.
//	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
//	if err != nil {
//		t.Fatal("PutObject err:", err)
//	}
//	t.Log("PutObject Success!")
//
//	//PutBucketLifecycle:Sets lifecycle configuration for your bucket. If a lifecycle configuration exists, it replaces it.
//	putPut := &s3.PutBucketLifecycleConfigurationInput{
//		Bucket: aws.String(TEST_BUCKET),
//		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
//			Rules: []*s3.LifecycleRule{
//				{
//					Expiration: &s3.LifecycleExpiration{
//						Days: aws.Int64(1),
//					},
//					Filter: &s3.LifecycleRuleFilter{
//						Prefix: aws.String(""),
//					},
//					ID:     aws.String("test"),
//					Status: aws.String("Enabled"),
//				},
//			},
//		},
//	}
//	_, err = sc.Client.PutBucketLifecycleConfiguration(putPut)
//	if err != nil {
//		t.Fatal("PutBucketLifecycle err:", err)
//	}
//	t.Log("PutBucketLifecycle Success!")
//
//	//GetBucketLifecycle:Returns the lifecycle configuration information set on the bucket.
//	getPut := &s3.GetBucketLifecycleConfigurationInput{
//		Bucket: aws.String(TEST_BUCKET),
//	}
//	_, err = sc.Client.GetBucketLifecycleConfiguration(getPut)
//	if err != nil {
//		t.Fatal("GetBucketLifecycle err:", err)
//	}
//	t.Log("GetBucketLifecycle Success!")
//
//	//Get object before lc.
//	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY)
//	if err != nil {
//		t.Fatal("GetObject before lc err:", err)
//	}
//	if v != TEST_VALUE {
//		t.Fatal("GetObject before lc err: value is:", v, ", but should be:", TEST_VALUE)
//	}
//	t.Log("GetObject before lc Success value:", v)
//
//	//Test "lifecycle.go".
//	err = os.Chdir("../../")
//	if err != nil {
//		t.Fatal("change dir in lc err:", err)
//	}
//	cmd := exec.Command("make", "runlc")
//	err = cmd.Run()
//	if err != nil {
//		t.Fatal("lc err:", err)
//	}
//	time.Sleep(time.Second * 3)
//	t.Log("lc Success!")
//	os.Chdir("../test/go")
//
//	//Get object after lc.
//	v, err = sc.GetObject(TEST_BUCKET, TEST_KEY)
//	if err != nil {
//		str := err.Error()
//		if strings.Contains(str, "NoSuchKey: The specified key does not exist") {
//			t.Log("GetObject after lc test Success:", err)
//		} else {
//			t.Fatal("GetObject after lc test Fail!", err)
//		}
//	} else {
//		t.Fatal("GetObject after lc test Fail!", err)
//	}
//
//	//DeleteBucketLifecycle:Deletes the lifecycle configuration from the bucket.
//	deletePut := &s3.DeleteBucketLifecycleInput{
//		Bucket: aws.String(TEST_BUCKET),
//	}
//	_, err = sc.Client.DeleteBucketLifecycle(deletePut)
//	if err != nil {
//		t.Fatal("DeleteBucketLifecycle err:", err)
//	}
//	t.Log("DeleteBucketLifecycle Success!")
//
//}
//
//func Test_LC_End(t *testing.T) {
//	sc := NewS3()
//	err := sc.DeleteObject(TEST_BUCKET, TEST_KEY)
//	if err != nil {
//		t.Log("DeleteObject err:", err)
//	}
//	err = sc.DeleteBucket(TEST_BUCKET)
//	if err != nil {
//		t.Fatal("DeleteBucket err:", err)
//		panic(err)
//	}
//
//}
