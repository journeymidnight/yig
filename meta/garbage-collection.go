package meta

import (
	"encoding/binary"
	"time"
	"bytes"
)

type GarbageCollection struct {
	BucketName	string
	ObjectName	string
	Location 	string
	Pool		string
	ObjectId	string
	Parts		map[int]Part
}

func GarbageCollectionFromObject(o Object) (gc GarbageCollection){
	gc.BucketName = o.BucketName
	gc.ObjectName = o.Name
	gc.Location = o.Location
	gc.Pool = o.Pool
	gc.ObjectId = o.ObjectId
	gc.Parts = o.Parts
	return
}

func (gc GarbageCollection) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		GARBAGE_COLLECTION_COLUMN_FAMILY: map[string][]byte{
			"location": []byte(gc.Location),
			"pool":     []byte(gc.Pool),
			"oid":      []byte(gc.ObjectId),
		},
	}
	if len(gc.Parts) != 0 {
		values[GARBAGE_COLLECTION_PART_COLUMN_FAMILY], err = ValuesForParts(gc.Parts)
		if err != nil {
			return
		}
	}
	return
}

// Rowkey format:
// bigEndian(unixNanoTimestamp) + BucketName + ObjectName
func (gc GarbageCollection) GetRowkey() (string, error) {
	var rowkey bytes.Buffer
	err := binary.Write(&rowkey, binary.BigEndian,
		uint64(time.Now().UnixNano()))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(gc.BucketName)
	rowkey.WriteString(gc.ObjectName)
	return rowkey.String(), nil
}