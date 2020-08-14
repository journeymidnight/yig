package api

import (
	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/brand"
	"net/http"
)

func getAclFromHeader(h http.Header, brandName Brand) (acl Acl, err error) {
	acl.CannedAcl = h.Get(brandName.GetGeneralFieldFullName(XACL))
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	err = IsValidCannedAcl(acl)
	return
}
