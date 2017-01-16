package api

import (
	. "git.letv.cn/yig/yig/api/datatype"
	"net/http"
)

func getAclFromHeader(h http.Header) (acl Acl, err error) {
	acl.CannedAcl = h.Get("x-amz-acl")
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	err = IsValidCannedAcl(acl)
	return
}