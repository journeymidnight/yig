package _go

import (
"bufio"
"encoding/xml"
"github.com/journeymidnight/yig/api/datatype"
. "github.com/journeymidnight/yig/test/go/lib"
"net/http"
"os"
"strings"
"testing"
"log"
)

const timeLayoutStr = "2006-01-02 15"

const (
	AclPublicXmlPlugin = `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
)

var logger *log.Logger

func Test_PluginJudge(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")
	err = sc.PutObject(TEST_BUCKET, TEST_KEY, TEST_VALUE)
	if err != nil {
		t.Fatal("PutObject err:", err)
		panic(err)
	}
	t.Log("PutObject Success.")
	url := GenTestObjectUrl(sc) + "?X-Oss-Referer=cdn"
	var policy = &datatype.AccessControlPolicy{}
	err = xml.Unmarshal([]byte(AclPublicXmlPlugin), policy)
	if err != nil {
		t.Fatal("PutObjectPublicAclWithXml err:", err)
	}
	acl := TransferToS3AccessControlPolicy(policy)
	if acl == nil {
		t.Fatal("PutObjectPublicAclWithXml err:", "empty acl!")
	}
	err = sc.PutObjectAclWithXml(TEST_BUCKET, TEST_KEY, acl)
	if err != nil {
		t.Fatal("PutObjectAclWithXml err:", err)
	}
	t.Log("PutObjectAclWithXml Success!")
	// After set public-read ACL.
	statusCode, data, err := HTTPRequestToGetObject(url)
	if err != nil {
		t.Fatal("GetObject err:", err)
	}
	if statusCode != http.StatusOK {
		t.Fatal("StatusCode should be STATUS_OK(200), but the code is:", statusCode)
	}
	t.Log("Get object value:", string(data))
	Path, _ := os.Getwd()
	YigPath := strings.Split(Path,"/")
	var PathGo string
	PathGo = ""
	for i := 0 ;i < len(YigPath); i++ {
		if YigPath[i] != "yig" {
			PathGo = PathGo + "/" + YigPath[i]
		}else {
			PathGo = PathGo + "/" + YigPath[i]
			break
		}
	}
	filePath := PathGo + "/access.log"
	f, err := os.Open(filePath)
	if err != nil {
		t.Fatal("[ERROR] Read file", filePath, "error:", err)
		return
	}
	t.Log("Read file success!")
	defer f.Close()
	scp := bufio.NewScanner(f)
	for scp.Scan() {
		line := scp.Text()
		data := strings.Split(line, " ")
		Lenth := len(data)
		methed := "GET"
		req := "/mybucket/testput?X-Oss-Referer=cdn"
		if data[2] == methed && data[3] == req {
			cdn := data[Lenth-1]
			if cdn != "true"{
				t.Fatal("The CDN value:", cdn)
				return
			}
		}
	}
	t.Log("CDN is true")
	err = sc.DeleteObject(TEST_BUCKET, TEST_KEY)
	if err != nil {
		t.Fatal("DeleteObject err:", err)
	}
	t.Log("Delete object success")
	err = sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
	}
	t.Log("Delete bucket success")
}
