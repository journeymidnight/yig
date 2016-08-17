package api

import (
	. "git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/helper"
	"net/http"
)

var validCannedAcl = []string{
	"private",
	"public-read",
	"public-read-write",
	"aws-exec-read",
	"authenticated-read",
	"bucket-owner-read",
	"bucket-owner-full-controll",
}

func getAclFromHeader(h http.Header) (acl Acl, err error) {
	acl.CannedAcl = h.Get("x-amz-acl")
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	if !helper.StringInSlice(acl.CannedAcl, validCannedAcl) {
		err = ErrInvalidCannedAcl
		return
	}
	return
}
