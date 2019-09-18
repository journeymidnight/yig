package types

type ObjMap struct {
	Rowkey     []byte // Rowkey cache
	Name       string
	BucketName string
	NullVerNum uint64
	NullVerId  string
}

