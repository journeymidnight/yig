package meta

type Part struct {
	Location string
	Pool string
	Size int64
	ObjectId string
	Offset int64  // offset of this part in whole object, omitted in multipart table
	Etag string
	LastModified string // time in format "2006-01-02T15:04:05.000Z"
}