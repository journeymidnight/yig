package types

type Users struct {
	OwnerId    string
	BucketName string
	Standard   uint64
	StandardIa uint64
	Glacier    uint64
}
