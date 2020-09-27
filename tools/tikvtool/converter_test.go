package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/journeymidnight/yig/meta/types"
)

var (
	bucketDML  = `('dntest1','{\"CannedAcl\": \"private\"}','{\"CorsRules\": null}','{\"DeleteTime\": \"\", \"LoggingEnabled\": {\"TargetBucket\": \"\", \"TargetPrefix\": \"\"}, \"SetLog\": false, \"SetTime\": \"\"}','{\"Rules\": null, \"XMLName\": {\"Local\": \"\", \"Space\": \"\"}}','96ce0e98-8680-41fd-a88e-964617e078c6','{\"Statement\": null, \"Version\": \"\"}','{\"ErrorDocument\": null, \"IndexDocument\": null, \"RedirectAllRequestsTo\": null, \"RoutingRules\": null, \"XMLName\": {\"Local\": \"\", \"Space\": \"\"}, \"Xmlns\": \"\"}','{\"Rules\": null, \"XMLName\": {\"Local\": \"\", \"Space\": \"\"}}','2020-09-25 08:20:47',0,'Disabled'),`
	objectDML  = `('kkk4','testput',16849096864726811403,'','','310f2b61-8a88-45b3-a386-b8c571c2230c',7,'','2020-08-17 06:53:28','','','null','{\"CannedAcl\": \"\"}',0,1,'',NULL,NULL,0,0,1597647208982740212),`
	objectDML2 = `('dntest1','drainer',0,'2e6f0ad8-c742-4a34-821c-cc44b7841350','tiger','96ce0e98-8680-41fd-a88e-964617e078c6',55053972,'','2020-09-25 08:21:48','67088180269d3b7df8dc678230b3674f-11','application/x-executable','{\"Content-Type\": \"application/x-executable\", \"X-Amz-Meta-S3cmd-Attrs\": \"atime:1601014536/ctime:1601014547/gid:0/gname:root/md5:10cde9c3ac217296d4790cfc424f5746/mode:33261/mtime:1601014547/uid:0/uname:root\"}','{\"CannedAcl\": \"private\"}',1,0,'',x'',NULL,2,0,1601022108964240458);`
	objectDML3 = `('kkk4','testput',16849096863125638626,'a957271c-4582-4b85-902d-cf89bc345c8b','tiger','310f2b61-8a88-45b3-a386-b8c571c2230c',15728640,'','2020-08-17 06:53:30','31fa0239031913958dd0a78ccc6c91b1-3','application/octet-stream','{}','{"CannedAcl": "private"}',0,0,'',x'',NULL,2,3,1597647210583912989),`
)

func print(data interface{}) {
	jsonData, _ := json.Marshal(data)
	fmt.Println(string(jsonData))
}

func Test_Parse(t *testing.T) {
	reflectMap = LoadPidToUidMap("/Users/cuixiaotian/test_map")
	var err error
	var bucket types.Bucket
	err = parseBucket([]byte(bucketDML), &bucket)
	if err != nil {
		t.Fatal("parse Bucket err:", err)
	}
	print(bucket)
	var object types.Object
	err = parseObject([]byte(objectDML), &object)
	if err != nil {
		t.Fatal("parse Objects err:", err)
	}
	print(object)

	err = parseObject([]byte(objectDML2), &object)
	if err != nil {
		t.Fatal("parse Objects err:", err)
	}
	print(object)

	err = parseObject([]byte(objectDML3), &object)
	if err != nil {
		t.Fatal("parse Objects err:", err)
	}
	print(object)
}
