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

func Test_Parse(t *testing.T) {
	var bucket types.Bucket
	err := parseBucket([]byte(bucketDML), &bucket)

	if err != nil {
		t.Fatal("parse Bucket err:", err)
	}
	jsonData, _ := json.Marshal(b)
	fmt.Println(string(jsonData))
	jsonData, _ = json.Marshal(bucket)
	fmt.Println(string(jsonData))
	o, err := parseObject([]byte(objectDML))
	if err != nil {
		t.Fatal("parse Objects err:", err)
	}
	jsonData, _ = json.Marshal(o)
	fmt.Println(string(jsonData))

	ho, err := parseObject([]byte(hotobjectsDML))
	if err != nil {
		t.Fatal("parse hot Objects err:", err)
	}
	jsonData, _ = json.Marshal(ho)
	fmt.Println(string(jsonData))

	dm, err := parseObject([]byte(deleteMarkerDML))
	if err != nil {
		t.Fatal("parse delete marker err:", err)
	}
	jsonData, _ = json.Marshal(dm)
	fmt.Println(string(jsonData))
}
