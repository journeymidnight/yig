package _go

import (
	"encoding/xml"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/test/go/lib"
	"testing"
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
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}

	err = sc.PutObjectWithStorageClass(TEST_BUCKET, TEST_KEY, TEST_VALUE, TEST_STORAGEGLACIER)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	t.Log("PutObject Success!")
}

func Test_RestoreObject(t *testing.T) {
	sc := NewS3()

	var config = &datatype.Restore{}
	err := xml.Unmarshal([]byte(RESTOREXML1), config)
	if err != nil {
		t.Fatal("Unmarshal encryption configuration err:", err)
	}

	restoreRequest := TransferToS3AccessRestoreRequest(config)
	if restoreRequest == nil {
		t.Fatal("RestoreObject err:", "empty restoreRequest!")
	}

	err = sc.RestoreObject(TEST_BUCKET, TEST_KEY, restoreRequest)
	if err != nil {
		t.Fatal("RestoreObject err:", err)
	}
	t.Log("RestoreObject Success!")

	err = sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
	t.Log("DeleteObject Success!")
	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
	}
	t.Log("DeleteBucket Success!")
}
