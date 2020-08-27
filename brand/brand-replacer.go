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
	XContentSha                  GeneralFieldName = "-Content-Sha256"
	XCredential                  GeneralFieldName = "-Credential"
	XAlgorithm                   GeneralFieldName = "-Algorithm"
	XACL                         GeneralFieldName = "-Acl"
	XDate                        GeneralFieldName = "-Date"
	XMeta                        GeneralFieldName = "-Meta"
	XExpires                     GeneralFieldName = "-Expires"
	XSignature                   GeneralFieldName = "-Signature"
	XSignedHeaders               GeneralFieldName = "-SignedHeaders"
	XSecurityToken               GeneralFieldName = "-Security-Token"
	XStorageClass                GeneralFieldName = "-Storage-Class"
	XServerSideEncryption        GeneralFieldName = "-Server-Side-Encryption"                                // ServerSideEncryption is the general AWS SSE HTTP header key.
	XSSEKmsContext               GeneralFieldName = "-Context"                                               // SSEKmsContext is the HTTP header key referencing the SSE-KMS encryption context.
	XSSECAlgorithm               GeneralFieldName = "-Server-Side-Encryption-Customer-Algorithm"             // SSECAlgorithm is the HTTP header key referencing the SSE-C algorithm.
	XSSECKey                     GeneralFieldName = "-Server-Side-Encryption-Customer-Key"                   // SSECKey is the HTTP header key referencing the SSE-C client-provided key..
	XSSECKeyMD5                  GeneralFieldName = "-Server-Side-Encryption-Customer-Key-Md5"               // SSECKeyMD5 is the HTTP header key referencing the MD5 sum of the client-provided key.
	XSSECopyAlgorithm            GeneralFieldName = "-Copy-Source-Server-Side-Encryption-Customer-Algorithm" // SSECopyAlgorithm is the HTTP header key referencing the SSE-C algorithm for SSE-C copy requests.
	XSSECopyKey                  GeneralFieldName = "-Copy-Source-Server-Side-Encryption-Customer-Key"       // SSECopyKey is the HTTP header key referencing the SSE-C client-provided key for SSE-C copy requests.
	XSSECopyKeyMD5               GeneralFieldName = "-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"   // SSECopyKeyMD5 is the HTTP header key referencing the MD5 sum of the client key for SSE-C copy requests.
	XRestore                     GeneralFieldName = "-Restore"
	XForbidOverwrite             GeneralFieldName = "-Forbid-Overwrite"
	XCopySource                  GeneralFieldName = "-Copy-Source"
	XCopySourceRange             GeneralFieldName = "-Copy-Source-Range"
	XCopySourceIfModifiedSince   GeneralFieldName = "-Copy-Source-If-Modified-Since"
	XCopySourceIfUnmodifiedSince GeneralFieldName = "-Copy-Source-If-Unmodified-Since"
	XCopySourceIfMatch           GeneralFieldName = "-Copy-Source-If-Match"
	XCopySourceIfNoneMatch       GeneralFieldName = "-Copy-Source-If-None-Match"
	XCopySourceVersionId         GeneralFieldName = "-Copy-Source-Version-Id"
	XRenameSourceKey             GeneralFieldName = "-Rename-Source-Key"
	XDecodedContentLength        GeneralFieldName = "-Decoded-Content-Length"
	XMetadataDirective           GeneralFieldName = "-Metadata-Directive"
	XVersionId                   GeneralFieldName = "-Version-Id"
	XNextAppendPosition          GeneralFieldName = "-Next-Append-Position"
	XDeleteMarker                GeneralFieldName = "-Delete-Marker"
	XObjectType                  GeneralFieldName = "-Object-Type"
	XID2                         GeneralFieldName = "-Id-2"
	XRequestId                   GeneralFieldName = "-Request-Id"
)

type SpecialFieldName string

const (
	SpecialName            SpecialFieldName = ""
	AccessKeyId            SpecialFieldName = "AccessKeyId"
	SignV2Algorithm        SpecialFieldName = ""
	SignV4Algorithm        SpecialFieldName = "4-HMAC-SHA256"
	SignV4ChunkedAlgorithm SpecialFieldName = "4-HMAC-SHA256-PAYLOAD"
	SignV4                 SpecialFieldName = "4"
	SignRequest            SpecialFieldName = "4_request" // need strings.ToLower
	Chunked                SpecialFieldName = "-chunked"
	StreamingContentSHA256 SpecialFieldName = "STREAMING"
	SSEAlgorithmKMS        SpecialFieldName = ":kms"        // SSEAlgorithmKMS is the value of 'X-***-Server-Side-Encryption' for SSE-KMS.
	SSEKmsID               SpecialFieldName = "-Kms-Key-Id" // SSEKmsID is the HTTP header key referencing the SSE-KMS key ID.
)

type Brand interface {
	BrandName() string
	GetGeneralFieldFullName(field GeneralFieldName) string
	GetSpecialFieldFullName(field SpecialFieldName) string
}

// Verify if request header field is X-Amz or X-UOS
// if can not find, UOS by default
func DistinguishBrandName(r *http.Request, postValues map[string]string) Brand {
	if postValues != nil {
		for field, value := range postValues {
			if strings.Contains(strings.ToLower(field), "x-amz-") ||
				strings.Contains(value, "AWS") || strings.Contains(strings.ToLower(value), "x-amz-") {
				return &Aws{
					Name: AWSName,
				}
			} else if strings.Contains(strings.ToLower(field), "x-uos-") ||
				strings.Contains(value, "UOS") || strings.Contains(strings.ToLower(value), "x-uos-") {
				return &Uos{
					Name: UOSName,
				}
			}
		}
		return &Uos{
			Name: UOSName,
		}
	}
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
