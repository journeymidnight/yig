package meta

import (
	"context"
	"encoding/hex"
	"log"

	"git.letv.cn/yig/yig/helper"
	"github.com/cannium/gohbase"
	"github.com/xxtea/xxtea-go/xxtea"
)

const (
	BUCKET_TABLE                          = "buckets"
	BUCKET_COLUMN_FAMILY                  = "b"
	BUCKET_ACL_COLUMN_FAMILY              = "a"
	BUCKET_CORS_COLUMN_FAMILY             = "c"
	USER_TABLE                            = "users"
	USER_COLUMN_FAMILY                    = "u"
	OBJECT_TABLE                          = "objects"
	OBJECT_COLUMN_FAMILY                  = "o"
	OBJECT_PART_COLUMN_FAMILY             = "p"
	GARBAGE_COLLECTION_TABLE              = "garbageCollection"
	GARBAGE_COLLECTION_COLUMN_FAMILY      = "gc"
	GARBAGE_COLLECTION_PART_COLUMN_FAMILY = "p"
	MULTIPART_TABLE                       = "multiparts"
	MULTIPART_COLUMN_FAMILY               = "m"
	CLUSTER_TABLE                         = "cluster"
	CLUSTER_COLUMN_FAMILY                 = "c"
	OBJMAP_TABLE                          = "objMap"
	OBJMAP_COLUMN_FAMILY                  = "om"

	CREATE_TIME_LAYOUT = "2006-01-02T15:04:05.000Z"

	ENCRYPTION_KEY_LENGTH        = 32 // 32 bytes for AES-"256"
	INITIALIZATION_VECTOR_LENGTH = 16 // 12 bytes is best performance for GCM, but for CTR
)

var (
	XXTEA_KEY         = []byte("hehehehe")
	SSE_S3_MASTER_KEY = []byte("hehehehehehehehehehehehehehehehe") // 32 bytes to select AES-256
	RootContext       = context.Background()
)

type Meta struct {
	Hbase  gohbase.Client
	Logger *log.Logger
	Cache  MetaCache
}

func New(logger *log.Logger, cacheEnabled bool) *Meta {
	var hbaseClient gohbase.Client
	znodeOption := gohbase.SetZnodeParentOption(helper.CONFIG.HbaseZnodeParent)
	hbaseClient = gohbase.NewClient(helper.CONFIG.ZookeeperAddress, znodeOption)
	meta := Meta{
		Hbase:  hbaseClient,
		Logger: logger,
		Cache:  newMetaCache(cacheEnabled),
	}
	return &meta
}

func Decrypt(value string) (string, error) {
	bytes, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(xxtea.Decrypt(bytes, XXTEA_KEY)), nil
}

func Encrypt(value string) string {
	return hex.EncodeToString(xxtea.Encrypt([]byte(value), XXTEA_KEY))
}
