package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/journeymidnight/yig/meta/types"
)

var bucketDML = `('mybucket',CONVERT('{\"CannedAcl\": \"private\"}' USING UTF8MB4),CONVERT('{\"CorsRules\": null}' USING UTF8MB4),CONVERT('{\"DeleteTime\": \"\", \"LoggingEnabled\": {\"TargetBucket\": \"\", \"TargetPrefix\": \"\"}, \"SetLog\": false, \"SetTime\": \"\"}' USING UTF8MB4),CONVERT('{\"Rules\": null, \"XMLName\": {\"Local\": \"\", \"Space\": \"\"}}' USING UTF8MB4),'310f2b61-8a88-45b3-a386-b8c571c2230c',CONVERT('{\"Statement\": null, \"Version\": \"\"}' USING UTF8MB4),CONVERT('{\"ErrorDocument\": null, \"IndexDocument\": null, \"RedirectAllRequestsTo\": null, \"RoutingRules\": null, \"XMLName\": {\"Local\": \"\", \"Space\": \"\"}, \"Xmlns\": \"\"}' USING UTF8MB4),CONVERT('{\"Rules\": null, \"XMLName\": {\"Local\": \"\", \"Space\": \"\"}}' USING UTF8MB4),'2020-08-11 06:49:34',0,'Disabled'),`
var hotobjectsDML = `('shenbei01','222222222_1435218821.wmv',0,'a957271c-4582-4b85-902d-cf89bc345c8b','rabbit','310f2b61-8a88-45b3-a386-b8c571c2230c',54849518,'212995:12:32768:1:8388608','2020-08-28 06:05:10','c5fe26a323d528d05422523aaf8e34c0','application/x-www-form-urlencoded',CONVERT('{\"Content-Type\": \"application/x-www-form-urlencoded\", \"md5Sum\": \"c5fe26a323d528d05422523aaf8e34c0\"}' USING UTF8MB4),CONVERT('{\"CannedAcl\": \"private\"}' USING UTF8MB4),1,0,'','',NULL,1,0,1598594710813575050);`
var objectDML = `('vv1','testhehe/testheihei',16849089987716300950,'a957271c-4582-4b85-902d-cf89bc345c8b','tiger','310f2b61-8a88-45b3-a386-b8c571c2230c',1048576,'212281:71','2020-08-17 08:48:05','7202826a7791073fe2787f0c94603278','',CONVERT('{\"md5Sum\": \"7202826a7791073fe2787f0c94603278\"}' USING UTF8MB4),CONVERT('{\"CannedAcl\": \"public-read\"}' USING UTF8MB4),0,0,'','',NULL,0,0,1597654085993250665),`
var deleteMarkerDML = `('kkk4','COPYED:testforbid',0,'','','310f2b61-8a88-45b3-a386-b8c571c2230c',17,'','2020-08-17 08:17:56','','',CONVERT('null' USING UTF8MB4),CONVERT('{\"CannedAcl\": \"\"}' USING UTF8MB4),0,1,'',NULL,NULL,0,0,1597652275979115163),`
var restoreDML = `('vv1','testhehe/testheihei',16849089987167531417,2,3,'2020-08-17 08:48:06','a957271c-4582-4b85-902d-cf89bc345c8b','turtle','310f2b61-8a88-45b3-a386-b8c571c2230c',1048576,'212281:73','7202826a7791073fe2787f0c94603278',0,1597654086542020198);`
var multipartDML = `('yanhuan','grep',16847793552822393149,'71f3bf77-c520-4a16-a68c-c7635ee099e4','71f3bf77-c520-4a16-a68c-c7635ee099e4','text/plain','af542a32-eb36-4dd7-a16a-d00536647460','tiger',CONVERT('{\"CannedAcl\": \"public-read\"}' USING UTF8MB4),CONVERT('{\"CopySourceSseCustomerAlgorithm\": \"\", \"CopySourceSseCustomerKey\": null, \"SseAwsKmsKeyId\": \"\", \"SseContext\": \"\", \"SseCustomerAlgorithm\": \"\", \"SseCustomerKey\": null, \"Type\": \"\"}' USING UTF8MB4),NULL,NULL,CONVERT('{\"Content-Type\": \"text/plain\", \"X-Amz-Meta-S3cmd-Attrs\": \"atime:1590479125/ctime:1590479005/gid:0/gname:root/md5:54ea0b8af669760341e1af350c0af2d6/mode:33188/mtime:1590479005/uid:0/uname:root\"}' USING UTF8MB4),0);`
var multipartpartDML = `(1,52428800,'23054576:5',0,'66a127e2b5866787a8499bfeab424b07','2020-09-01 08:55:22',NULL,'yanhuan','grep',16847793552822393149),`
var objectpartDML = `(4,5242880,'212281:95',15728640,'45118e5c7f590889921a473a1b1e280f','2020-08-18 05:29:31',NULL,'kkk4','tikv',16849015501389947708)`
var restoreobjectpartDML = `(9,5242880,'534219:15',41943040,'71d78ed4642d7e687073e0ca11d9b884','2020-08-11 07:44:43',NULL,'mybucket','efh/yigv13',16849612186130448260);`

func print(data interface{}) {
	jsonData, _ := json.Marshal(data)
	fmt.Println(string(jsonData))
}

func Test_Parse(t *testing.T) {
	reflectMap = LoadPidToUidMap("./map")
	var err error
	var bucket types.Bucket
	err = parseBucket([]byte(bucketDML), &bucket)
	if err != nil {
		t.Fatal("parse Bucket err:", err)
	}
	print(bucket)

	var object, hotobject, deleteMarker types.Object
	err = parseObject([]byte(objectDML), &object)
	if err != nil {
		t.Fatal("parse Objects err:", err)
	}
	print(object)

	err = parseObject([]byte(hotobjectsDML), &hotobject)
	if err != nil {
		t.Fatal("parse hot Objects err:", err)
	}
	print(hotobject)

	err = parseObject([]byte(deleteMarkerDML), &deleteMarker)
	if err != nil {
		t.Fatal("parse delete marker err:", err)
	}
	print(deleteMarker)

	var restore types.Freezer
	err = parseRestore([]byte(restoreDML), &restore)
	if err != nil {
		t.Fatal("parse restore err:", err)
	}
	print(restore)

	var multipartpart, objectpart, restoreobjectpart types.Part
	err = parseObjectPart([]byte(multipartpartDML), &multipartpart)
	if err != nil {
		t.Fatal("parse multipart part err:", err)
	}
	print(multipartpart)

	err = parseObjectPart([]byte(objectpartDML), &objectpart)
	if err != nil {
		t.Fatal("parse object part err:", err)
	}
	print(objectpart)

	err = parseObjectPart([]byte(restoreobjectpartDML), &restoreobjectpart)
	if err != nil {
		t.Fatal("parse restoreobject part err:", err)
	}
	print(restoreobjectpart)

	var multipart types.Multipart
	err = parseMultiparts([]byte(multipartDML), &multipart)
	if err != nil {
		t.Fatal("parse multiparts err:", err)
	}
	print(multipart)
}
