package api

import (
	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/brand"
	"net/http"
)

func getAclFromHeader(h http.Header, brand Brand) (acl Acl, err error) {
	acl.CannedAcl = h.Get(brand.GetHeaderFieldKey(XACL))
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	err = IsValidCannedAcl(acl)
	return
}
