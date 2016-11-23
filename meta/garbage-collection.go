package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"git.letv.cn/yig/yig/helper"
	"github.com/cannium/gohbase/hrpc"
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
	Status     string // status of this entry, in Pending/Deleting
	Parts      map[int]*Part
	TriedTimes int
}

func GarbageCollectionFromObject(o *Object) (gc GarbageCollection) {
	gc.BucketName = o.BucketName
	gc.ObjectName = o.Name
	gc.Location = o.Location
	gc.Pool = o.Pool
	gc.ObjectId = o.ObjectId
	gc.Status = "Pending"
	gc.Parts = o.Parts
	gc.TriedTimes = 0
	return
}

func GarbageCollectionFromResponse(response *hrpc.Result) (garbage GarbageCollection, err error) {
	garbage = GarbageCollection{}
	garbage.Parts = make(map[int]*Part)
	for _, cell := range response.Cells {
		garbage.Rowkey = string(cell.Row)
		switch string(cell.Family) {
		case GARBAGE_COLLECTION_COLUMN_FAMILY:
			switch string(cell.Qualifier) {
			case "location":
				garbage.Location = string(cell.Value)
			case "pool":
				garbage.Pool = string(cell.Value)
			case "oid":
				garbage.ObjectId = string(cell.Value)
			case "status":
				garbage.Status = string(cell.Value)
			case "tried":
				garbage.TriedTimes, err = strconv.Atoi(string(cell.Value))
				if err != nil {
					return
				}
			}
		case GARBAGE_COLLECTION_PART_COLUMN_FAMILY:
			var partNumber int
			partNumber, err = strconv.Atoi(string(cell.Qualifier))
			if err != nil {
				return
			}
			var p Part
			err = json.Unmarshal(cell.Value, &p)
			if err != nil {
				return
			}
			garbage.Parts[partNumber] = &p
		}
	}
	return garbage, nil
}

func (gc GarbageCollection) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		GARBAGE_COLLECTION_COLUMN_FAMILY: map[string][]byte{
			"location": []byte(gc.Location),
			"pool":     []byte(gc.Pool),
			"oid":      []byte(gc.ObjectId),
			"status":   []byte(gc.Status),
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

// Insert object to `garbageCollection` table
func (m *Meta) PutObjectToGarbageCollection(object *Object) error {
	garbageCollection := GarbageCollectionFromObject(object)

	garbageCollectionValues, err := garbageCollection.GetValues()
	if err != nil {
		return err
	}
	garbageCollectionRowkey, err := garbageCollection.GetRowkey()
	if err != nil {
		return err
	}
	putRequest, err := hrpc.NewPutStr(
		context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout),
		GARBAGE_COLLECTION_TABLE,
		garbageCollectionRowkey, garbageCollectionValues)
	if err != nil {
		return err
	}
	_, err = m.Hbase.Put(putRequest)
	return err
}

func (m *Meta) ScanGarbageCollection(limit int) ([]GarbageCollection, error) {
	scanRequest, err := hrpc.NewScanStr(
		context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout),
		GARBAGE_COLLECTION_TABLE,
		hrpc.NumberOfRows(uint32(limit)))
	if err != nil {
		return nil, err
	}
	scanResponse, err := m.Hbase.Scan(scanRequest)
	if err != nil {
		return nil, err
	}
	objectsToRemove := make([]GarbageCollection, len(scanResponse))
	for _, result := range scanResponse {
		garbage, err := GarbageCollectionFromResponse(result)
		if err != nil {
			return nil, err
		}
		objectsToRemove = append(objectsToRemove, garbage)
	}
	return objectsToRemove, nil
}

func (m *Meta) RemoveGarbageCollection(garbage GarbageCollection) error {
	deleteRequest, err := hrpc.NewDelStr(
		context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout),
		GARBAGE_COLLECTION_TABLE,
		garbage.Rowkey, garbage.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = m.Hbase.Delete(deleteRequest)
	return err
}
