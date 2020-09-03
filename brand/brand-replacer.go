package brand

import (
	"net/http"
	"strings"
)

// Refer: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
var CommonS3ResponseHeaders = []string{"Content-Length", "Content-Type", "Connection", "Date", "ETag", "Server"}

// startWithConds - map which indicates if a given condition supports starts-with policy operator
var StartsWithConds = map[string]bool{
	"$acl":                     true,
	"$bucket":                  false,
	"$cache-control":           true,
	"$content-type":            true,
	"$content-disposition":     true,
	"$content-encoding":        true,
	"$expires":                 true,
	"$key":                     true,
	"$success_action_redirect": true,
	"$redirect":                true,
	"$success_action_status":   false,
}

type BrandKeyType string

const BrandKey BrandKeyType = "Brand"

var Brands = []string{"AWS"}

type HeaderFieldKey string

const (
	AccessKeyId                  HeaderFieldKey = "AccessKeyId"
	XGeneralName                 HeaderFieldKey = "-"
	XContentSha                  HeaderFieldKey = "-Content-Sha256"
	XCredential                  HeaderFieldKey = "-Credential"
	XAlgorithm                   HeaderFieldKey = "-Algorithm"
	XACL                         HeaderFieldKey = "-Acl"
	XDate                        HeaderFieldKey = "-Date"
	XMeta                        HeaderFieldKey = "-Meta-"
	XExpires                     HeaderFieldKey = "-Expires"
	XSignature                   HeaderFieldKey = "-Signature"
	XSignedHeaders               HeaderFieldKey = "-SignedHeaders"
	XSecurityToken               HeaderFieldKey = "-Security-Token"
	XStorageClass                HeaderFieldKey = "-Storage-Class"
	XServerSideEncryption        HeaderFieldKey = "-Server-Side-Encryption"                                // ServerSideEncryption is the general AWS SSE HTTP header key.
	XSSEKmsContext               HeaderFieldKey = "-Context"                                               // SSEKmsContext is the HTTP header key referencing the SSE-KMS encryption context.
	XSSECAlgorithm               HeaderFieldKey = "-Server-Side-Encryption-Customer-Algorithm"             // SSECAlgorithm is the HTTP header key referencing the SSE-C algorithm.
	XSSECKey                     HeaderFieldKey = "-Server-Side-Encryption-Customer-Key"                   // SSECKey is the HTTP header key referencing the SSE-C client-provided key..
	XSSECKeyMD5                  HeaderFieldKey = "-Server-Side-Encryption-Customer-Key-Md5"               // SSECKeyMD5 is the HTTP header key referencing the MD5 sum of the client-provided key.
	XSSECopyAlgorithm            HeaderFieldKey = "-Copy-Source-Server-Side-Encryption-Customer-Algorithm" // SSECopyAlgorithm is the HTTP header key referencing the SSE-C algorithm for SSE-C copy requests.
	XSSECopyKey                  HeaderFieldKey = "-Copy-Source-Server-Side-Encryption-Customer-Key"       // SSECopyKey is the HTTP header key referencing the SSE-C client-provided key for SSE-C copy requests.
	XSSECopyKeyMD5               HeaderFieldKey = "-Copy-Source-Server-Side-Encryption-Customer-Key-Md5"   // SSECopyKeyMD5 is the HTTP header key referencing the MD5 sum of the client key for SSE-C copy requests.
	SSEKmsID                     HeaderFieldKey = "-Kms-Key-Id"                                            // SSEKmsID is the HTTP header key referencing the SSE-KMS key ID.
	XRestore                     HeaderFieldKey = "-Restore"
	XForbidOverwrite             HeaderFieldKey = "-Forbid-Overwrite"
	XCopySource                  HeaderFieldKey = "-Copy-Source"
	XCopySourceRange             HeaderFieldKey = "-Copy-Source-Range"
	XCopySourceIfModifiedSince   HeaderFieldKey = "-Copy-Source-If-Modified-Since"
	XCopySourceIfUnmodifiedSince HeaderFieldKey = "-Copy-Source-If-Unmodified-Since"
	XCopySourceIfMatch           HeaderFieldKey = "-Copy-Source-If-Match"
	XCopySourceIfNoneMatch       HeaderFieldKey = "-Copy-Source-If-None-Match"
	XCopySourceVersionId         HeaderFieldKey = "-Copy-Source-Version-Id"
	XRenameSourceKey             HeaderFieldKey = "-Rename-Source-Key"
	XDecodedContentLength        HeaderFieldKey = "-Decoded-Content-Length"
	XMetadataDirective           HeaderFieldKey = "-Metadata-Directive"
	XVersionId                   HeaderFieldKey = "-Version-Id"
	XNextAppendPosition          HeaderFieldKey = "-Next-Append-Position"
	XDeleteMarker                HeaderFieldKey = "-Delete-Marker"
	XObjectType                  HeaderFieldKey = "-Object-Type"
	XID2                         HeaderFieldKey = "-Id-2"
	XRequestId                   HeaderFieldKey = "-Request-Id"
)

type HeaderFieldValue string

const (
	SpecialName            HeaderFieldValue = ""
	SignV2Algorithm        HeaderFieldValue = ""
	SignV4Algorithm        HeaderFieldValue = "4-HMAC-SHA256"
	SignV4ChunkedAlgorithm HeaderFieldValue = "4-HMAC-SHA256-PAYLOAD"
	SignV4                 HeaderFieldValue = "4"
	SignRequest            HeaderFieldValue = "4_request" // need strings.ToLower
	Chunked                HeaderFieldValue = "-chunked"
	StreamingContentSHA256 HeaderFieldValue = "STREAMING"
	SSEAlgorithmKMS        HeaderFieldValue = ":kms" // SSEAlgorithmKMS is the value of 'X-***-Server-Side-Encryption' for SSE-KMS.
)

func Initialize(brands []string) {
	for _, brand := range brands {
		name := strings.ToUpper(brand)
		if name == "AWS" {
			continue
		}
		Brands = append(Brands, name)
	}
	initPolicyConditions()
	initCommonResponseHeaders()
}

func initPolicyConditions() {
	var startsWithXConds = map[HeaderFieldKey]bool{
		XAlgorithm:  false,
		XCredential: false,
		XDate:       false,
	}
	for _, brand := range Brands {
		if brand == "AWS" {
			for key, value := range startsWithXConds {
				StartsWithConds["$"+strings.ToLower("X-Amz"+string(key))] = value
			}
		} else {
			for key, value := range startsWithXConds {
				StartsWithConds["$"+strings.ToLower("X-"+brand+string(key))] = value
			}
		}
	}
}

func initCommonResponseHeaders() {
	var commonS3ResponseXHeaders = []HeaderFieldKey{XDeleteMarker, XID2, XRequestId, XVersionId}
	for _, brand := range Brands {
		if brand == "AWS" {
			for _, value := range commonS3ResponseXHeaders {
				CommonS3ResponseHeaders = append(CommonS3ResponseHeaders, strings.ToLower("X-Amz"+string(value)))
			}
		} else {
			for _, value := range commonS3ResponseXHeaders {
				CommonS3ResponseHeaders = append(CommonS3ResponseHeaders, strings.ToLower("X-"+brand+string(value)))
			}
		}
	}
}

type Brand interface {
	BrandName() string
	GetHeaderFieldKey(field HeaderFieldKey) string
	GetHeaderFieldValue(field HeaderFieldValue) string
}

// Verify if request header field is X-Amz or X-UOS
// if can not find, UOS by default
func DistinguishBrandName(r *http.Request, postValues map[string]string) (brand Brand) {
	if postValues != nil {
		for field, value := range postValues {
			brand = getBrand(field, value)
			if brand != nil {
				return brand
			}
		}
		return &Aws{
			Name: "AWS",
		}
	}
	for field, value := range r.URL.Query() {
		brand = getBrand(field, value[0])
		if brand != nil {
			return brand
		}
	}
	for field, value := range r.Header {
		brand = getBrand(field, value[0])
		if brand != nil {
			return brand
		}
	}
	return &Aws{
		Name: "AWS",
	}
}

func getBrand(k, v string) Brand {
	if strings.Contains(strings.ToUpper(k), "AMZ") || strings.Contains(strings.ToUpper(v), "AWS") {
		return &Aws{
			Name: "AWS",
		}
	} else {
		for _, brand := range Brands {
			if brand == "AWS" {
				continue
			} else {
				if strings.Contains(strings.ToUpper(k), brand) || strings.Contains(strings.ToUpper(v), brand) {
					return &GeneralBrand{
						Name: brand,
					}
				}
			}
		}
	}
	return nil
}

type Aws struct {
	Name string
}

func (a *Aws) BrandName() string {
	return a.Name
}

// get full name by unified name
func (a *Aws) GetHeaderFieldKey(field HeaderFieldKey) string {
	switch field {
	case SSEKmsID:
		return "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	case AccessKeyId:
		return a.Name + string(field)
	default:
		return "X-Amz" + string(field)
	}
}

func (a *Aws) GetHeaderFieldValue(field HeaderFieldValue) string {
	switch field {
	case StreamingContentSHA256:
		return "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	default:
		return a.Name + string(field)
	}
}

type GeneralBrand struct {
	Name string
}

func (g *GeneralBrand) BrandName() string {
	return g.Name
}

// get full name by unified name
func (g *GeneralBrand) GetHeaderFieldKey(field HeaderFieldKey) string {
	switch field {
	case SSEKmsID:
		return "X-Uos-Server-Side-Encryption-Uos-Kms-Key-Id"
	case AccessKeyId:
		return g.Name + string(field)
	default:
		return http.CanonicalHeaderKey("X-" + g.Name + string(field))
	}
}

func (g *GeneralBrand) GetHeaderFieldValue(field HeaderFieldValue) string {
	switch field {
	case StreamingContentSHA256:
		return "STREAMING-UOS4-HMAC-SHA256-PAYLOAD"
	default:
		return g.Name + string(field)
	}
}

func GetContextBrand(r *http.Request) Brand {
	return r.Context().Value(BrandKey).(Brand)
}
