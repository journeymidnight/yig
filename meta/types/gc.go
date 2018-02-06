package types

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"time"
)

type GarbageCollection struct {
	Rowkey     string // rowkey cache
	BucketName string
	ObjectName string
	Location   string
	Pool       string
	ObjectId   string
	Status     string    // status of this entry, in Pending/Deleting
	MTime      time.Time // last modify time of status
	Parts      map[int]*Part
	TriedTimes int
}

func (gc GarbageCollection) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		GARBAGE_COLLECTION_COLUMN_FAMILY: map[string][]byte{
			"location": []byte(gc.Location),
			"pool":     []byte(gc.Pool),
			"oid":      []byte(gc.ObjectId),
			"status":   []byte(gc.Status),
			"mtime":    []byte(gc.MTime.Format(CREATE_TIME_LAYOUT)),
			"tried":    []byte(strconv.Itoa(gc.TriedTimes)),
		},
	}
	if len(gc.Parts) != 0 {
		values[GARBAGE_COLLECTION_PART_COLUMN_FAMILY], err = valuesForParts(gc.Parts)
		if err != nil {
			return
		}
	}
	return
}

func (gc GarbageCollection) GetValuesForDelete() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		GARBAGE_COLLECTION_COLUMN_FAMILY:      map[string][]byte{},
		GARBAGE_COLLECTION_PART_COLUMN_FAMILY: map[string][]byte{},
	}
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
