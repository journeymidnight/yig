package _go

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func Test_LifeCycle(t *testing.T) {
	sc := NewS3()

	//Create bucket.
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
	}
	t.Log("CreateBucket Success!")

	//Put object.
	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")

    //PutBucketLifecycle:Sets lifecycle configuration for your bucket. If a lifecycle configuration exists, it replaces it.
	putPut := &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(TEST_BUCKET),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(1),
					},
					Filter: &s3.LifecycleRuleFilter{
						Prefix: aws.String(""),
					},
					ID:     aws.String("test"),
					Status: aws.String("Enabled"),
				},
			},
		},
	}
	_, err = sc.Client.PutBucketLifecycleConfiguration(putPut)
	if err != nil {
		t.Fatal("PutBucketLifecycle err:", err)
	}
	t.Log("PutBucketLifecycle Success!")

	//GetBucketLifecycle:Returns the lifecycle configuration information set on the bucket.
	getPut := &s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(TEST_BUCKET),
	}
	_, err = sc.Client.GetBucketLifecycleConfiguration(getPut)
	if err != nil {
		t.Fatal("GetBucketLifecycle err:", err)
	}
	t.Log("GetBucketLifecycle Success!")

    //Get object before lc.
	v, err := sc.GetObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("GetObject before lc err:", err)
	}
	if v != TEST_VALUE {
		t.Fatal("GetObject before lc err: value is:", v, ", but should be:", TEST_VALUE)
	}
	t.Log("GetObject before lc Success value:", v)

	//Test "lc.go".
	err = os.Chdir("../../integrate")
	if err != nil {
                t.Fatal("change dir in lc err:", err)
        }
	cmd := exec.Command("bash", "runlc.sh")
	err = cmd.Run()
	if err != nil {
		t.Fatal("lc err:", err)
	}
	time.Sleep(time.Second * 3)
	t.Log("lc Success!")
    os.Chdir("../test/go")

	//Get object after lc.
	v, err = sc.GetObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		str := err.Error()
		if strings.Contains(str, "NoSuchKey: The specified key does not exist") {
			t.Log("GetObject after lc test Success:", err)
		} else {
			t.Fatal("GetObject after lc test Fail!", err)
		}
	}else{
		t.Fatal("GetObject after lc test Fail!", err)
	}

	//DeleteBucketLifecycle:Deletes the lifecycle configuration from the bucket.
	deletePut := &s3.DeleteBucketLifecycleInput{
		Bucket: aws.String(TEST_BUCKET),
	}
    _, err = sc.Client.DeleteBucketLifecycle(deletePut)
	if err != nil {
		t.Fatal("DeleteBucketLifecycle err:", err)
	}
	t.Log("DeleteBucketLifecycle Success!")

    //Delete bucket.
	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
	}
	t.Log("DeleteBucket Success.")
}


