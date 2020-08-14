package brand

import (
	"net/http"
	"strings"
)

type BrandNameKeyType string

const BrandNameKey BrandNameKeyType = "BrandName"

const (
	XAmzName = "X-Amz"
	AWSName  = "AWS"
	XUosName = "X-Uos"
	UOSName  = "UOS"
)

type GeneralFieldName string

const (
	XGeneralName                 GeneralFieldName = "-"
	XContentSha                                   = "-Content-Sha256"
	XCredential                                   = "-Credential"
	XAlgorithm                                    = "-Algorithm"
	XACL                                          = "-Acl"
	XDate                                         = "-Date"
	XMeta                                         = "-Meta"
	XExpires                                      = "-Expires"
	XSignature                                    = "-Signature"
	XSignedHeaders                                = "-SignedHeaders"
	XSecurityToken                                = "-Security-Token"
	XStorageClass                                 = "-Storage-Class"
	XServerSideEncryption                         = "-Server-Side-Encryption"                                // ServerSideEncryption is the general AWS SSE HTTP header key.
	XSSEKmsContext                                = "-Context"                                               // SSEKmsContext is the HTTP header key referencing the SSE-KMS encryption context.
	XSSECAlgorithm                                = "-Server-Side-Encryption-Customer-Algorithm"             // SSECAlgorithm is the HTTP header key referencing the SSE-C algorithm.
	XSSECKey                                      = "-Server-Side-Encryption-Customer-Key"                   // SSECKey is the HTTP header key referencing the SSE-C client-provided key..
	XSSECKeyMD5                                   = "-Server-Side-Encryption-Customer-Key-Md5"               // SSECKeyMD5 is the HTTP header key referencing the MD5 sum of the client-provided key.
	XSSECopyAlgorithm                             = "-Copy-Source-Server-Side-Encryption-Customer-Algorithm" // SSECopyAlgorithm is the HTTP header key referencing the SSE-C algorithm for SSE-C copy requests.
	XSSECopyKey                                   = "-Copy-Source-Server-Side-Encryption-Customer-Key"       // SSECopyKey is the HTTP header key referencing the SSE-C client-provided key for SSE-C copy requests.
	XSSECopyKeyMD5                                = "-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"   // SSECopyKeyMD5 is the HTTP header key referencing the MD5 sum of the client key for SSE-C copy requests.
	XRestore                                      = "-Restore"
	XForbidOverwrite                              = "-Forbid-Overwrite"
	XCopySource                                   = "-Copy-Source"
	XCopySourceRange                              = "-Copy-Source-Range"
	XCopySourceIfModifiedSince                    = "-Copy-Source-If-Modified-Since"
	XCopySourceIfUnmodifiedSince                  = "-Copy-Source-If-Unmodified-Since"
	XCopySourceIfMatch                            = "-Copy-Source-If-Match"
	XCopySourceIfNoneMatch                        = "-Copy-Source-If-None-Match"
	XCopySourceVersionId                          = "-Copy-Source-Version-Id"
	XRenameSourceKey                              = "-Rename-Source-Key"
	XDecodedContentLength                         = "-Decoded-Content-Length"
	XMetadataDirective                            = "-Metadata-Directive"
	XVersionId                                    = "-Version-Id"
	XNextAppendPosition                           = "-Next-Append-Position"
	XDeleteMarker                                 = "-Delete-Marker"
	XObjectType                                   = "-Object-Type"
	XID2                                          = "-Id-2"
	XRequestId                                    = "-Request-Id"
)

type SpecialFieldName string

const (
	SpecialName            SpecialFieldName = ""
	AccessKeyId                             = "AccessKeyId"
	SignV2Algorithm                         = ""
	SignV4Algorithm                         = "4-HMAC-SHA256"
	SignV4ChunkedAlgorithm                  = "4-HMAC-SHA256-PAYLOAD"
	SignV4                                  = "4"
	SignRequest                             = "4_request" // need strings.ToLower
	Chunked                                 = "-chunked"
	StreamingContentSHA256                  = "STREAMING"
	SSEAlgorithmKMS                         = ":kms"        // SSEAlgorithmKMS is the value of 'X-***-Server-Side-Encryption' for SSE-KMS.
	SSEKmsID                                = "-Kms-Key-Id" // SSEKmsID is the HTTP header key referencing the SSE-KMS key ID.
)

type Brand interface {
	BrandName() string
	GetGeneralFieldFullName(field GeneralFieldName) string
	GetSpecialFieldFullName(field SpecialFieldName) string
}

// Verify if request header field is X-Amz or X-UOS
// if can not find, UOS by default
func DistinguishBrandName(r *http.Request) Brand {
	for field, value := range r.Header {
		if "authorization" == strings.ToLower(field) {
			if strings.Contains(value[0], "AWS") || strings.Contains(strings.ToLower(value[0]), "x-amz-") {
				return &Aws{
					Name: AWSName,
				}
			} else if strings.Contains(value[0], "UOS") || strings.Contains(strings.ToLower(value[0]), "x-uos-") {
				return &Uos{
					Name: UOSName,
				}
			}
		}

		if strings.Contains(strings.ToLower(field), "x-amz-") {
			return &Aws{
				Name: AWSName,
			}
		} else if strings.Contains(strings.ToLower(field), "x-uos-") {
			return &Uos{
				Name: UOSName,
			}
		}
	}
	return &Uos{
		Name: UOSName,
	}
}

type Aws struct {
	Name string
}

func (a *Aws) BrandName() string {
	return a.Name
}

// get full name by unified name
func (a *Aws) GetGeneralFieldFullName(field GeneralFieldName) string {
	return XAmzName + string(field)
}

func (a *Aws) GetSpecialFieldFullName(field SpecialFieldName) string {
	switch field {
	case StreamingContentSHA256:
		return "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	case SSEKmsID:
		return "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	default:
		return AWSName + string(field)
	}
}

type Uos struct {
	Name string
}

func (u *Uos) BrandName() string {
	return u.Name
}

// get full name by unified name
func (u *Uos) GetGeneralFieldFullName(field GeneralFieldName) string {
	return XUosName + string(field)
}

func (u *Uos) GetSpecialFieldFullName(field SpecialFieldName) string {
	switch field {
	case StreamingContentSHA256:
		return "STREAMING-UOS4-HMAC-SHA256-PAYLOAD"
	case SSEKmsID:
		return "X-Uos-Server-Side-Encryption-Uos-Kms-Key-Id"
	default:
		return UOSName + string(field)
	}
}

func GetContextBrand(r *http.Request) Brand {
	return r.Context().Value(BrandNameKey).(Brand)
}
