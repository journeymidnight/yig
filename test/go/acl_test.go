package _go

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/test/go/lib"
)

const (
	AclPrivateXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
				<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclPublicXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				</Grantee>
				<Permission>READ</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclPublicReadWriteXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				</Grantee>
				<Permission>READ</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
				</Grantee>
				<Permission>WRITE</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclSpecifyObjectReadXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hahahaha</ID>
				</Grantee>
				<Permission>READ</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclSpecifyBucketReadXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hahahaha</ID>
				</Grantee>
				<Permission>READ</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclSpecifyBucketReadAndWriteXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hahahaha</ID>
				</Grantee>
				<Permission>READ</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hahahaha</ID>
				</Grantee>
				<Permission>WRITE</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclSpecifyBucketFullControlXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hahahaha</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`

	AclLogDeliveryXml = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Owner>
			<ID>hehehehe</ID>
		</Owner>
		<AccessControlList>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
					<ID>hehehehe</ID>
				</Grantee>
				<Permission>FULL_CONTROL</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/s3/LogDelivery</URI>
				</Grantee>
				<Permission>READ_ACP</Permission>
			</Grant>
			<Grant>
				<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
					<URI>http://acs.amazonaws.com/groups/s3/LogDelivery</URI>
				</Grantee>
				<Permission>WRITE</Permission>
			</Grant>
		</AccessControlList>
	</AccessControlPolicy>`
)

func Test_ACL(t *testing.T) {
	sc := NewS3()
	defer func() {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")
	err = sc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}
	t.Log("PutObject Success.")

	err = sc.PutBucketAcl(TestBucket, BucketCannedACLPrivate)
	if err != nil {
		t.Fatal("PutBucketAcl err:", err)
	}
	t.Log("PutBucketAcl Success!")
	out, err := sc.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	err = sc.PutObjectAcl(TestBucket, TestKey+"NONE", BucketCannedACLPrivate)
	if err == nil {
		t.Fatal("PutObjectAclWithNoSuchObject err: We have no such key", TestKey)
	}
	t.Log("PutObjectAclWithNoSuchObject Success!")

	err = sc.PutObjectAcl(TestBucket, TestKey, BucketCannedACLPrivate)
	if err != nil {
		t.Fatal("PutObjectAcl err:", err)
	}
	t.Log("PutObjectAcl Success!")

	out, err = sc.GetObjectAcl(TestBucket, TestKey)
	if err != nil {
		t.Fatal("GetObjectAcl err:", err)
	}
	t.Log("GetObjectAcl Success! out:", out)

	// This test case is used to test whether the result of obtaining an Object by an external user is correct
	// before setting the public-read ACL and setting the public-read ACL.
	url := GenTestObjectUrl(sc)
	// before set public-read ACL.
	statusCode, _, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("GetObject Without public-read ACL test Success.")

	// set public-read ACL.
	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclPublicXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutObjectAclWithXml(TestBucket, TestKey, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
	}
	t.Log("PutObjectAclWithXml Success!")

	out, err = sc.GetObjectAcl(TestBucket, TestKey)
	if err != nil {
		t.Fatal("GetObjectAcl err:", err)
	}
	t.Log("GetObjectAcl Success! out:", out)

	// After set public-read ACL.
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	// This test case is used to test whether the result of obtaining an Object by an external user is correct
	// before setting the private ACL and setting the private ACL.
	url = GenTestObjectUrl(sc)

	// before set private ACL.
	statusCode, data, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	//set private ACL.
	policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclPrivateXml), policy)
	if err != nil {
		t.Fatal("PutObjectPrivateAclWithXml err:", err)
	}
	acl = TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPrivateAclWithXml err:", "empty acl!")
	}
	err = sc.PutObjectAclWithXml(TestBucket, TestKey, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
	}
	t.Log("PutObjectAclWithXml Success!")

	out, err = sc.GetObjectAcl(TestBucket, TestKey)
	if err != nil {
		t.Fatal("GetObjectAcl err:", err)
	}
	t.Log("GetObjectAcl Success! out:", out)

	// After set private ACL.
	statusCode, _, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("GetObject With private ACL test Success.", statusCode)

	// Try delete object
	statusCode, _, err = HTTPRequestToDeleteObject(url)
	if err != nil {
		t.Fatal("Delete Object err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("DeleteObject With private ACL test Success.")

	// Set bucket acl public read and write
	err = sc.PutBucketAcl(TestBucket, BucketCannedACLPublicReadWrite)
	if err != nil {
		t.Fatal("PutBucketAcl err:", err)
	}
	t.Log("PutBucketAcl PublicReadWrite Success!")
	out, err = sc.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl PublicReadWrite Success! out:", out)

	// check object still exist
	statusCode, data, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))

	// Try delete object again
	statusCode, _, err = HTTPRequestToDeleteObject(url)
	if err != nil {
		t.Fatal("Delete Object err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusNoContent {
		t.Fatal("StatusCode should be StatusNoContent(204), but the code is:", statusCode)
	}
	t.Log("DeleteObject With bucket ReadWrite test Success.")

	// try put object in anonymous
	statusCode, data, err = HTTPRequestToPutObject(url, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be StatusOK(200), but the code is:", statusCode)
	}
	t.Log("Put object test success")

	// before set private ACL.
	statusCode, data, err = HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be STATUS_OK
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))
}

func Test_ACL_Specify_ID_Object_Read(t *testing.T) {
	sc := NewS3()
	sch := NewS3Ha()
	defer func() {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")
	err = sc.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}
	t.Log("PutObject Success.")

	// set public-read ACL.
	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclSpecifyObjectReadXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutObjectAclWithXml(TestBucket, TestKey, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
		panic(err)
	}
	t.Log("PutObjectAclWithXml Success!")

	out, err := sc.GetObjectAcl(TestBucket, TestKey)
	if err != nil {
		t.Fatal("GetObjectAcl err:", err)
		panic(err)
	}
	t.Log("GetObjectAcl Success! out:", out)

	data, err := sch.GetObject(TestBucket, TestKey)
	if err != nil || data != TestValue {
		t.Fatal("GetObject err:", err)
		panic(err)
	}
	t.Log("GetObject with hahahaha Success.")

	// construct a anonymous get request
	url := GenTestObjectUrl(sc)

	// send request
	statusCode, _, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	//StatusCode should be AccessDenied
	if statusCode != http.StatusForbidden {
		t.Fatal("StatusCode should be AccessDenied(403), but the code is:", statusCode)
	}
	t.Log("GetObject With anonymous is forbidden and test Success.")

	//test delete StatusCode should be AccessDenied
	err = sch.DeleteObject(TestBucket, TestKey)
	if err == nil {
		t.Fatal("DeleteObject test with hahahaha should be faild, but success")
	}
	t.Log("DeleteObject test with hahahaha Success.", err)
}

func Test_ACL_Specify_ID_Bucket_Read(t *testing.T) {
	sc := NewS3()
	sch := NewS3Ha()
	defer func() {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	_, err = sch.ListObjects(TestBucket, "", "", 100)
	if err == nil {
		t.Fatal("ListObjects should be forbidden", err)
		panic(err)
	}
	t.Log("ListObjects forbidden test Success.")

	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclSpecifyBucketReadXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutBucketAclWithXml(TestBucket, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
		panic(err)
	}
	t.Log("PutBucketAclWithXml Success!")

	out, err := sc.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	err = sch.PutObject(TestBucket, TestKey, TestValue)
	if err == nil {
		t.Fatal("PutObject should be forbidden")
		panic("PutObject should be forbidden")
	}
	t.Log("PutObject forbidden test Success.")

	_, err = sch.ListObjects(TestBucket, "", "", 100)
	if err != nil {
		t.Fatal("ListObjects should be allowed", err)
		panic(err)
	}
	t.Log("ListObjects test Success.")

	_, err = sch.GetObject(TestBucket, TestKey)
	if err == nil {
		t.Fatal("GetObject should failed but successed:", err)
		panic(err)
	}
	t.Log("GetObject test Success.")
}

func Test_ACL_Specify_ID_Bucket_ReadWrite(t *testing.T) {
	sc := NewS3()
	sch := NewS3Ha()
	defer func() {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclSpecifyBucketReadAndWriteXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutBucketAclWithXml(TestBucket, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
		panic(err)
	}
	t.Log("PutBucketAclWithXml Success!")

	out, err := sc.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	err = sch.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject should be allowed", err)
		panic("PutObject should be allowed")
	}
	t.Log("PutObject allowed test Success.")

	_, err = sch.ListObjects(TestBucket, "", "", 100)
	if err != nil {
		t.Fatal("ListObjects should be allowed", err)
		panic(err)
	}
	t.Log("ListObjects test Success.")

	data, err := sch.GetObject(TestBucket, TestKey)
	if err != nil || data != TestValue {
		t.Fatal("GetObject should success:", err)
		panic(err)
	}
	t.Log("GetObject test Success.")
}

func Test_ACL_Specify_ID_Bucket_FullControl(t *testing.T) {
	sc := NewS3()
	sch := NewS3Ha()
	defer func() {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclSpecifyBucketFullControlXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutBucketAclWithXml(TestBucket, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
		panic(err)
	}
	t.Log("PutBucketAclWithXml Success!")

	out, err := sc.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	err = sch.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject should be allowed", err)
		panic("PutObject should be allowed")
	}
	t.Log("PutObject allowed test Success.")

	out, err = sch.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	data, err := sc.GetObject(TestBucket, TestKey)
	if err != nil || data != TestValue {
		t.Fatal("GetObject should success:", err)
		panic(err)
	}
	t.Log("GetObject test Success.")
}

func Test_ACL_Specify_ID_Bucket_Log_Delivery(t *testing.T) {
	sc := NewS3()
	sch := NewS3Ha()
	defer func() {
		sc.DeleteObject(TestBucket, TestKey)
		sc.DeleteBucket(TestBucket)
	}()
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclLogDeliveryXml), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutBucketAclWithXml(TestBucket, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
		panic(err)
	}
	t.Log("PutBucketAclWithXml Success!")

	out, err := sc.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	err = sch.PutObject(TestBucket, TestKey, TestValue)
	if err != nil {
		t.Fatal("PutObject should be allowed", err)
		panic("PutObject should be allowed")
	}
	t.Log("PutObject allowed test Success.")

	out, err = sch.GetBucketAcl(TestBucket)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success! out:", out)

	data, err := sc.GetObject(TestBucket, TestKey)
	if err != nil || data != TestValue {
		t.Fatal("GetObject should success:", err)
		panic(err)
	}
	t.Log("GetObject test Success.")
}
