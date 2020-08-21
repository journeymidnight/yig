package datatype

import (
	"github.com/journeymidnight/yig/iam/common"
	"net/http"
	"testing"
)

const (
	TESTDATE = "20200821T014732Z"
)

func TestCallbackAuthSignature(t *testing.T) {
	credition := common.Credential{
		AccessKeyID:     "hehehehe",
		SecretAccessKey: "hehehehe",
	}
	signature := getSignatureForCallback(credition, TESTDATE)
	if signature != "UOS-CALLBACK-AUTH hehehehe:T9DnvduiBm9I4fQEz/SCQYpSlVQ=" {
		t.Error("The calculated signature does not match the expected")
		return
	}
	t.Log("The calculated signature and the expected match")
}

func TestGetImageInfoFromReader(t *testing.T) {
	resp, err := http.Get("https://www.baidu.com/img/bd_logo1.png")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	height, width, imageType := GetImageInfoFromReader(resp.Body)
	if height == 0 || width == 0 || imageType == "" {
		t.Errorf("Failed to get picture information!")
	}
	t.Log("Get picture information successfully!", height, width, imageType)
}
