package _go

import (
	"net/http"
	"os"
	"testing"

	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_Bucket_Prepare(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
}

func Test_PutBucketPolicy(t *testing.T) {
	sc := NewS3()
	err := sc.PutBucketPolicy(TEST_BUCKET, GetObjectPolicy_1)
	if err != nil {
		t.Fatal("PutBucketPolicy err:", err)
	}
	t.Log("PutBucketPolicy success.")
}

func Test_GetBucketPolicy(t *testing.T) {
	sc := NewS3()
	result, err := sc.GetBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err, "policy:", result)
	}

	var p policy.Policy
	err = p.UnmarshalJSON([]byte(result))
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err, "policy:", result)
	}
	t.Log("GetBucketPolicy success.")
	err = sc.DeleteBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketPolicy err:", err)
	}

	result, err = sc.GetBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err)
	}

	var p2 policy.Policy
	err = p2.UnmarshalJSON([]byte(result))
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err, "policy:", result)
	}

	if len(p2.Statements) != 0 {
		t.Fatal("DeleteBucketPolicy err:", "policy:", result)
	}
	t.Log("DeleteBucketPolicy success.")

}

// This test case is used to test whether the result of obtaining an Object by an external user is correct
// before setting the bucket policy and setting the bucket policy.
func Test_BucketPolicySample(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteObject(TEST_BUCKET, TEST_KEY)
		sc.DeleteBucket(TEST_BUCKET)
	}()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
	}
	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}

	//Anonymous to get
	url := "http://" + *sc.Client.Config.Endpoint + string(os.PathSeparator) + TEST_BUCKET + string(os.PathSeparator) + TEST_KEY
	statusCode, _, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}

	err = sc.PutBucketPolicy(TEST_BUCKET, GetObjectPolicy_1)
	if err != nil {
		t.Fatal("PutBucketPolicy err:", err)
	}

	policy, err := sc.GetBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketPolicy err:", err)
	}
	t.Log("Bucket policy:", Format(policy))

	// After set policy
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	err = sc.DeleteBucketPolicy(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketPolicy err:", err)
	}

	//After delete policy
	statusCode, _, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
}

// Test different situation with access policy when anonymous access;
// Situation 1:BucketPolicy Allow Gentlemen; 		BucketACL Private;		ObjectACL Private; 		legalRefererUrl GetObject should be OK; commonRefererUrl GetObject should be Failed;
// Situation 2:BucketPolicy Allow NotLike Thief; 	BucketACL Private;		ObjectACL Private; 		commonRefererUrl GetObject should be OK; illegalRefererUrl GetObject should be Failed;
// Situation 3:BucketPolicy Deny Thief; 			BucketACL PublicRead; 	ObjectACL PublicRead; 	commonRefererUrl GetObject should be OK; illegalRefererUrl GetObject should be Failed;
// Situation 4:BucketPolicy Deny NotLike Gentlemen; BucketACL PublicRead; 	ObjectACL PublicRead; 	legalRefererUrl GetObject should be OK; commonRefererUrl GetObject should be Failed;
// Situation 5:BucketPolicy Allow IPAddress; 		BucketACL Private; 		ObjectACL Private; 		legalIP GetObject should be OK; commonIP GetObject should be Failed;
// Situation 6:BucketPolicy Allow NotIPAddress; 	BucketACL Private; 		ObjectACL Private; 		commonIP GetObject should be OK; illegalIP GetObject should be Failed;
// Situation 7:BucketPolicy Deny IPAddress; 		BucketACL PublicRead; 	ObjectACL PublicRead; 	commonIP GetObject should be OK; illegalIP GetObject should be Failed;
// Situation 8:BucketPolicy Deny NotIPAddress; 		BucketACL PublicRead; 	ObjectACL PublicRead; 	legalIP GetObject should be OK; commonIP GetObject should be Failed;
func Test_GetObjectByAnonymousWithPolicyCondition(t *testing.T) {
	sc := NewS3()
	sc.CleanEnv()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}

	illegalRefererUrl := TEST_ILLEGALREFERER + "ImThief/"
	legalRefererUrl := TEST_LEGALREFERER + "ImGentlemen/"
	commonRefererUrl := TEST_COMMONREFERER + "ImCommon/"

	illegalIP := "10.0.11.1"
	legalIP := "10.0.12.12"
	commonIP := "10.0.13.13"

	// Situation 1:BucketPolicy Allow Gentlemen; BucketACL Private;	ObjectACL Private; legalRefererUrl GetObject should be OK; commonRefererUrl GetObject should be Failed;
	PolicyWithRefererGroup1 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyAllowStringLike, BucketACL: BucketCannedACLPrivate, ObjectACL: ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup1, http.StatusOK, legalRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 1: SetBucketPolicyAllowStringLike, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup1, http.StatusForbidden, commonRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 1: SetBucketPolicyAllowStringLike, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 2:BucketPolicy Allow NotLike Thief; BucketACL Private;	ObjectACL Private; commonRefererUrl GetObject should be OK; illegalRefererUrl GetObject should be Failed;
	PolicyWithRefererGroup2 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyAllowStringNotLike, BucketACL: BucketCannedACLPrivate, ObjectACL: ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup2, http.StatusOK, commonRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 2: SetBucketPolicyAllowStringNotLike, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup2, http.StatusForbidden, illegalRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 2: SetBucketPolicyAllowStringNotLike, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 3:BucketPolicy Deny Thief; BucketACL PublicRead; ObjectACL PublicRead; commonRefererUrl GetObject should be OK; illegalRefererUrl GetObject should be Failed;
	PolicyWithRefererGroup3 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyDenyStringLike, BucketACL: BucketCannedACLPublicRead, ObjectACL: ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup3, http.StatusOK, commonRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 3: SetBucketPolicyDenyStringLike, BucketCannedACLPublicRead, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup3, http.StatusForbidden, illegalRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 3: SetBucketPolicyDenyStringLike, BucketCannedACLPublicRead, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}

	// Situation 4:BucketPolicy Deny NotLike Gentlemen; BucketACL PublicRead; ObjectACL PublicRead; legalRefererUrl GetObject should be OK; commonRefererUrl GetObject should be Failed;
	PolicyWithRefererGroup4 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyDenyStringNotLike, BucketACL: BucketCannedACLPublicRead, ObjectACL: ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup4, http.StatusOK, legalRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 4: SetBucketPolicyDenyStringNotLike, BucketCannedACLPublicRead, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup4, http.StatusForbidden, commonRefererUrl, HTTPRequestToGetObjectWithReferer)
	if err != nil {
		t.Log("Anonymous access situation 4: SetBucketPolicyDenyStringNotLike, BucketCannedACLPublicRead, ObjectCannedACLPublicRead Failed.")
		t.Fatal(err)
	}

	// Situation 5:BucketPolicy Allow IPAddress; BucketACL Private; ObjectACL Private; legalIP GetObject should be OK; commonIP GetObject should be Failed;
	PolicyWithRefererGroup5 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyAllowIPAddress, BucketACL: BucketCannedACLPrivate, ObjectACL: ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup5, http.StatusOK, legalIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 5: SetBucketPolicyAllowIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup5, http.StatusForbidden, commonIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 5: SetBucketPolicyAllowIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 6:BucketPolicy Allow NotIPAddress; BucketACL Private; ObjectACL Private; commonIP GetObject should be OK; illegalIP GetObject should be Failed;
	PolicyWithRefererGroup6 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyAllowNotIPAddress, BucketACL: BucketCannedACLPrivate, ObjectACL: ObjectCannedACLPrivate}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup6, http.StatusOK, commonIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 6: SetBucketPolicyAllowNotIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup6, http.StatusForbidden, illegalIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 6: SetBucketPolicyAllowNotIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 7:BucketPolicy Deny IPAddress; BucketACL PublicRead; ObjectACL PublicRead; commonIP GetObject should be OK; illegalIP GetObject should be Failed;
	PolicyWithRefererGroup7 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyDenyIPAddress, BucketACL: BucketCannedACLPublicRead, ObjectACL: ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup7, http.StatusOK, commonIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 7: SetBucketPolicyDenyIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup7, http.StatusForbidden, illegalIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 7: SetBucketPolicyDenyIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	// Situation 8:BucketPolicy Deny NotIPAddress; BucketACL PublicRead; ObjectACL PublicRead; legalIP GetObject should be OK; commonIP GetObject should be Failed;
	PolicyWithRefererGroup8 := AccessPolicyGroup{BucketPolicy: SetBucketPolicyDenyNotIPAddress, BucketACL: BucketCannedACLPublicRead, ObjectACL: ObjectCannedACLPublicRead}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup8, http.StatusOK, legalIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 8: SetBucketPolicyDenyNotIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}
	err = sc.TestAnonymousAccessResultWithPolicyCondition(PolicyWithRefererGroup8, http.StatusForbidden, commonIP, HTTPRequestToGetObjectWithSpecialIP)
	if err != nil {
		t.Log("Anonymous access situation 8: SetBucketPolicyDenyNotIPAddress, BucketCannedACLPrivate, ObjectCannedACLPrivate Failed.")
		t.Fatal(err)
	}

	sc.CleanEnv()
}
