# Table Example:

|       | Column Family Name 1(name explanation) | Column Family Name 2(name explanation) | ... | Column Family Name n(name explanation)|
|-------|----------------------------------------|----------------------------------------|-----|---------------------------------------|
|Row Key | Column Qualifiers 1(description): value type <br> Column Qualifiers 2(description): value type <br> ... <br> Column Qualifiers n(description): value type | Column Qualifiers 1(description): value type <br> Column Qualifiers 2(description): value type <br> ... <br> Column Qualifiers n(description): value type | ... | Column Qualifiers 1(description): value type <br> Column Qualifiers 2(description): value type <br> ... <br> Column Qualifiers n(description): value type|

# Note:
 - HBase doesn't care about value types, they are handled by YIG
 - HBase ensures row level atomicity(see https://hbase.apache.org/acid-semantics.html)
 
# Table `buckets`

|       | `b`(for buckets) | `a`(for ACL)|
|-------|----------------------------------------|----------------------------------------|
|BucketName | UID(owner user ID in IAM): string <br> ACL(canned ACL): string <br> CORS: Cors struct <br> LC: Lc struct <br> createTime: string in "2006-01-02T15:04:05.000Z" <br> versioning: Disabled ｜ Enabled ｜ Suspended <br> usage: uint64 | TBD|

# Table `objects`

|       | `o`(for objects) |  `p`(an object might be stored as several parts in Ceph)
|-------|----------------------------------------|----------------------------------------|
|BucketName + ObjectNameSeparator + ObjectName + ObjectNameSeparator +  <br> bigEndian(uint64.max - unixNanoTimestamp) <br> Note: <br> VersionId = XXTEA(unixNanoTimestamp) <br> ObjectNameSeparator = "\n" <br> unixNanoTimestamp: uint64 | bucket(bucket name): string <br> location(which Ceph cluster this object locates): string <br> pool(which Ceph pool this object locates, if this object has only one part): string <br> owner: UID <br> oid(object name in Ceph): string <br> size: uint64 <br> lastModified: string in "2006-01-02T15:04:05.000Z" <br> etag: string <br> content-type: string <br> attributes(user defined attributes, not interpreted by YIG): map[string]string <br> ACL(canned ACL): string <br> nullVersion: bool <br> deleteMarker: bool <br> sseType(server side encryption type): enum{"SSE-KMS", "SSE-S3", "SSE-C"} <br> encryptionKey(key for SSE-S3, itself encrypted with a master key): string <br> IV(initialization vector, empty for multiparts): string | 1:{location: string, pool: string, size: int64, etag: string, oid: string, offset: int64, IV: string} <br> 2:{location: string, pool: string, size: int64, etag: string, oid: string, offset: int64, IV: string} <br> ... <br> n:{location: string, pool: string, size: int64, etag: string, oid: string, offset: int64, IV: string}|

# Table `objMap`

|       | `om`(for objectMap)|
|-------|----------------------------------------|
|BucketName + ObjectNameSeparator + ObjectName + ObjectNameSeparator | nullVerNum: uint64 <br>  (nullVerNum = unixNanoTimestamp)|

# Table `users`

|       | `u`(for users)|
|-------|----------------------------------------|
|UID | bucketName 1:"" <br> bucketName 2:"" <br> ... <br> bucketName n:""|

# Table `multiparts`

|       | `m`(for multiparts)|
|-------|----------------------------------------|
|BucketName + bigEndian(uint16(count("/", ObjectName))) + ObjectName + bigEndian(unixNanoTimestamp) <br> <br> Note: <br> UploadId = XXTEA(unixNanoTimestamp) <br> unixNanoTimestamp: uint64 | 0: object metadata <br> 1: {location: string, pool: string, size: int64, etag: string, oid: string, IV: string} <br> 2: {location: string, pool: string, size: int64, etag: string, oid: string, IV: string} <br> ... <br> n: {location: string, pool: string, size: int64, etag: string, oid: string, IV: string}|

# Table `garbageCollection`

|       | `gc`(for garbage collection) | `p`(parts)|
|-------|----------------------------------------|--------|
|bigEndian(unixNanoTimestamp) + bucketName + objectName | location: string <br> pool: string <br> oid: string <br> status: Pending｜Deleting <br> tried(tried times): int | 1:{location: string, pool: string, size: int64, etag: string, oid: string, offset: int64} <br> 2:{location: string, pool: string, size: int64, etag: string, oid: string, offset: int64} <br> ... <br> n:{location: string, pool: string, size: int64, etag: string, oid: string, offset: int <br> (Same as in table `objects`)|

# Table `cluster`

|       | `c`(for cluster)|
|-------|----------------------------------------|
|fsid+ObjectNameSeparator+poolName | weight: int|

# Table `lifecycle`

|       | `lc`(for lifecycle)|
|-------|----------------------------------------|
|bucketName | TBD|
