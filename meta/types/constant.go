package types

const (
	CREATE_TIME_LAYOUT           = "2006-01-02T15:04:05.000Z"
	TIME_LAYOUT_TIDB             = "2006-01-02 15:04:05"
	INITIALIZATION_VECTOR_LENGTH = 16 // 12 bytes is best performance for GCM, but for CTR
	ObjectNameEnding             = ":"
	ObjectNameSeparator          = "\n"
	ObjectNameSmallestStr        = " "
	ResponseNumberOfRows         = 1024
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
	LIFE_CYCLE_TABLE                      = "lifeCycle"
	LIFE_CYCLE_COLUMN_FAMILY              = "lc"
	MULTIPART_TABLE                       = "multiparts"
	MULTIPART_COLUMN_FAMILY               = "m"
	CLUSTER_TABLE                         = "cluster"
	CLUSTER_COLUMN_FAMILY                 = "c"
	OBJMAP_TABLE                          = "objMap"
	OBJMAP_COLUMN_FAMILY                  = "om"
)

const (
	SYNC_EVENT_BUCKET_USAGE_PREFIX = "sync_bucket_usage_"
)

var (
	XXTEA_KEY         = []byte("hehehehe")
	SSE_S3_MASTER_KEY = []byte("hehehehehehehehehehehehehehehehe") // 32 bytes to select AES-256
)
