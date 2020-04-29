package datatype

import (
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

var ValidCannedAcl = []string{
	"private",
	"public-read",
	"public-read-write",
	"aws-exec-read",
	"authenticated-read",
	"bucket-owner-read",
	"bucket-owner-full-controll",
}

const (
	CANNEDACL_PRIVATE                    = 0
	CANNEDACL_PUBLIC_READ                = 1
	CANNEDACL_PUBLIC_READ_WRITE          = 2
	CANNEDACL_AWS_EXEC_READ              = 3
	CANNEDACL_AUTHENTICATED_READ         = 4
	CANNEDACL_BUCKET_OWNER_READ          = 5
	CANNEDACL_BUCKET_OWNER_FULL_CONTROLL = 6
)

const (
	XMLNSXSI = "http://www.w3.org/2001/XMLSchema-instance"
	XMLNS    = "http://s3.amazonaws.com/doc/2006-03-01/"
)

const (
	ACL_TYPE_CANONICAL_USER = "CanonicalUser"
	ACL_TYPE_GROUP          = "Group"
)

const (
	ACL_GROUP_TYPE_ALL_USERS           = "http://acs.amazonaws.com/groups/global/AllUsers"
	ACL_GROUP_TYPE_AUTHENTICATED_USERS = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
)

const (
	ACL_PERM_READ         = "READ"
	ACL_PERM_WRITE        = "WRITE"
	ACL_PERM_READ_ACP     = "READ_ACP"
	ACL_PERM_WRITE_ACP    = "WRITE_ACP"
	ACL_PERM_FULL_CONTROL = "FULL_CONTROL"
)

type Acl struct {
	CannedAcl string
	// TODO fancy ACLs
}

type AccessControlPolicy struct {
	XMLName           xml.Name `xml:"AccessControlPolicy"`
	Xmlns             string   `xml:"xmlns,attr,omitempty"`
	ID                string   `xml:"Owner>ID"`
	DisplayName       string   `xml:"Owner>DisplayName"`
	AccessControlList []Grant  `xml:"AccessControlList>Grant"`
}

type Grant struct {
	XMLName    xml.Name `xml:"Grant"`
	Grantee    Grantee  `xml:"Grantee"`
	Permission string   `xml:"Permission"`
}

type Grantee struct {
	XMLName      xml.Name `xml:"Grantee"`
	XmlnsXsi     string   `xml:"xmlns:xsi,attr"`
	XsiType      string   `xml:"http://www.w3.org/2001/XMLSchema-instance type,attr"`
	URI          string   `xml:"URI,omitempty"`
	ID           string   `xml:"ID,omitempty"`
	DisplayName  string   `xml:"DisplayName,omitempty"`
	EmailAddress string   `xml:"EmailAddress,omitempty"`
}

type AccessControlPolicyResponse struct {
	XMLName           xml.Name        `xml:"AccessControlPolicy"`
	Xmlns             string          `xml:"xmlns,attr,omitempty"`
	ID                string          `xml:"Owner>ID"`
	DisplayName       string          `xml:"Owner>DisplayName"`
	AccessControlList []GrantResponse `xml:"AccessControlList>Grant"`
}

type GrantResponse struct {
	XMLName    xml.Name        `xml:"Grant"`
	Grantee    GranteeResponse `xml:"Grantee"`
	Permission string          `xml:"Permission"`
}

type GranteeResponse struct {
	XMLName      xml.Name `xml:"Grantee"`
	XmlnsXsi     string   `xml:"xmlns:xsi,attr"`
	XsiType      string   `xml:"xsi:type,attr"`
	URI          string   `xml:"URI,omitempty"`
	ID           string   `xml:"ID,omitempty"`
	DisplayName  string   `xml:"DisplayName,omitempty"`
	EmailAddress string   `xml:"EmailAddress,omitempty"`
}

func IsValidCannedAcl(acl Acl) (err error) {
	if !helper.StringInSlice(acl.CannedAcl, ValidCannedAcl) {
		err = ErrInvalidCannedAcl
		return
	}
	return
}

// the function will be deleted, because we will use AccessControlPolicy instead canned acl stored in hbase
func GetCannedAclFromPolicy(policy AccessControlPolicy) (acl Acl, err error) {
	aclOwner := Owner{ID: policy.ID, DisplayName: policy.DisplayName}
	var canonicalUser bool
	var group bool
	for _, grant := range policy.AccessControlList {
		helper.Logger.Info("GetCannedAclFromPolicy")
		switch grant.Grantee.XsiType {
		case ACL_TYPE_CANONICAL_USER:
			if grant.Grantee.ID != aclOwner.ID {
				helper.Logger.Info("grant.Grantee.ID:", grant.Grantee.ID,
					"not equals aclOwner.ID:", aclOwner.ID)
				return acl, ErrUnsupportedAcl
			}
			if grant.Permission != ACL_PERM_FULL_CONTROL {
				helper.Logger.Info("grant.Permission:", grant.Permission,
					"not equals", ACL_PERM_FULL_CONTROL)
				return acl, ErrUnsupportedAcl
			}
			canonicalUser = true
		case ACL_TYPE_GROUP:
			if grant.Grantee.URI == ACL_GROUP_TYPE_ALL_USERS {
				helper.Logger.Info("grant.Grantee.URI is", ACL_GROUP_TYPE_ALL_USERS)
				if grant.Permission != ACL_PERM_READ {
					helper.Logger.Info("grant.Permission:", grant.Permission,
						"not equals", ACL_PERM_READ)
					return acl, ErrUnsupportedAcl
				}
				acl = Acl{CannedAcl: ValidCannedAcl[CANNEDACL_PUBLIC_READ]}
				group = true
			} else if grant.Grantee.URI == ACL_GROUP_TYPE_AUTHENTICATED_USERS {
				helper.Logger.Info("grant.Grantee.URI is",
					ACL_GROUP_TYPE_AUTHENTICATED_USERS)
				if grant.Permission != ACL_PERM_READ {
					helper.Logger.Info("grant.Permission:", grant.Permission,
						"not equals", ACL_PERM_FULL_CONTROL)
					return acl, ErrUnsupportedAcl
				}
				acl = Acl{CannedAcl: ValidCannedAcl[CANNEDACL_AUTHENTICATED_READ]}
				group = true
			} else {
				helper.Logger.Info("grant.Grantee.URI is invalid:", grant.Grantee.URI)
				return acl, ErrUnsupportedAcl
			}
		default:
			helper.Logger.Info("grant.Grantee.XsiType is invalid:", grant.Grantee.XsiType)
			return acl, ErrUnsupportedAcl
		}
	}

	if !canonicalUser {
		helper.Logger.Info("canonicalUser is invalid:", canonicalUser)
		return acl, ErrUnsupportedAcl
	}

	if !group {
		acl = Acl{CannedAcl: ValidCannedAcl[CANNEDACL_PRIVATE]}
	}

	return acl, nil
}

func createGrant(xsiType string, owner Owner, perm string, groupType string) (grant GrantResponse, err error) {

	if xsiType == ACL_TYPE_CANONICAL_USER {
		grant.Grantee.ID = owner.ID
		grant.Grantee.DisplayName = owner.DisplayName
	} else if xsiType == ACL_TYPE_GROUP {
		grant.Grantee.URI = groupType
	} else {
		return grant, ErrUnsupportedAcl
	}
	grant.Permission = perm
	grant.Grantee.XmlnsXsi = XMLNSXSI
	grant.Grantee.XsiType = xsiType
	return
}

func CreatePolicyFromCanned(owner Owner, bucketOwner Owner, acl Acl) (
	policy AccessControlPolicyResponse, err error) {

	policy.ID = owner.ID
	policy.DisplayName = owner.DisplayName
	policy.Xmlns = XMLNS
	grant, err := createGrant(ACL_TYPE_CANONICAL_USER, owner, ACL_PERM_FULL_CONTROL, "")
	if err != nil {
		return policy, err
	}
	policy.AccessControlList = append(policy.AccessControlList, grant)
	if acl.CannedAcl == "private" {
		return policy, nil
	}
	switch acl.CannedAcl {
	case "public-read":
		owner := Owner{}
		grant, err := createGrant(ACL_TYPE_GROUP, owner, ACL_PERM_READ, ACL_GROUP_TYPE_ALL_USERS)
		if err != nil {
			return policy, err
		}
		policy.AccessControlList = append(policy.AccessControlList, grant)
	case "public-read-write":
		owner := Owner{}
		grant, err := createGrant(ACL_TYPE_GROUP, owner, ACL_PERM_READ, ACL_GROUP_TYPE_ALL_USERS)
		if err != nil {
			return policy, err
		}
		policy.AccessControlList = append(policy.AccessControlList, grant)
		grant, err = createGrant(ACL_TYPE_GROUP, owner, ACL_PERM_WRITE, ACL_GROUP_TYPE_ALL_USERS)
		if err != nil {
			return policy, err
		}
		policy.AccessControlList = append(policy.AccessControlList, grant)
	case "authenticated-read":
		owner := Owner{}
		grant, err := createGrant(ACL_TYPE_GROUP, owner, ACL_PERM_READ, ACL_GROUP_TYPE_AUTHENTICATED_USERS)
		if err != nil {
			return policy, err
		}
		policy.AccessControlList = append(policy.AccessControlList, grant)
	case "bucket-owner-read":
		grant, err := createGrant(ACL_TYPE_CANONICAL_USER, bucketOwner, ACL_PERM_READ, "")
		if err != nil {
			return policy, err
		}
		if bucketOwner.ID != owner.ID {
			policy.AccessControlList = append(policy.AccessControlList, grant)
		}
	case "bucket-owner-full-control":
		grant, err := createGrant(ACL_TYPE_CANONICAL_USER, bucketOwner, ACL_PERM_FULL_CONTROL, "")
		if err != nil {
			return policy, err
		}
		if bucketOwner.ID != owner.ID {
			policy.AccessControlList = append(policy.AccessControlList, grant)
		}
	default:
		return policy, ErrUnsupportedAcl
	}
	return
}
