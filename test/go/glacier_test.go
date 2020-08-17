package _go

import (
	"encoding/xml"
	"testing"

	"github.com/journeymidnight/aws-sdk-go/service/s3"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/test/go/lib"
)

const (
	RESTOREXML1 = `<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Days>1</Days>     
		<GlacierJobParameters>
			<Tier>Expedited</Tier>    
		</GlacierJobParameters>
	</RestoreRequest>`

	RESTOREXML2 = `<RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Days>2</Days>     
		<GlacierJobParameters>
			<Tier>Expedited</Tier>    
		</GlacierJobParameters>
	</RestoreRequest>`
)

func Test_PutObjectWithGlacier(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestBucket)
	defer sc.CleanEnv()
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.PutObjectWithStorageClass(TestBucket, TestKey, TestValue, s3.StorageClassGlacier)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")
}

func Test_RestoreObject(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TestBucket)
	defer sc.CleanEnv()
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.PutObjectWithStorageClass(TestBucket, TestKey, TestValue, s3.ObjectStorageClassGlacier)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")
	var config = &datatype.Restore{}
	err = xml.Unmarshal([]byte(RESTOREXML1), config)
	if err != nil {
		t.Fatal("Unmarshal encryption configuration err:", err)
	}

	restoreRequest := TransferToS3AccessRestoreRequest(config)
	if restoreRequest == nil {
		t.Fatal("RestoreObject err:", "empty restoreRequest!")
	}

	err = sc.RestoreObject(TestBucket, TestKey, restoreRequest)
	if err != nil {
		t.Fatal("RestoreObject err:", err)
	}
	t.Log("RestoreObject Success!")

	err = sc.DeleteObject(TestBucket, TestKey)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
	t.Log("DeleteObject Success!")
	err = sc.DeleteBucket(TestBucket)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
	}
	t.Log("DeleteBucket Success!")
}
