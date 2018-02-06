package hbaseclient

import (
	"context"
	"encoding/json"
	"github.com/cannium/gohbase/hrpc"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"strconv"
	"time"
)

func (h *HbaseClient) PutObjectToGarbageCollection(object *Object) error {
	garbageCollection := GarbageCollectionFromObject(object)

	garbageCollectionValues, err := garbageCollection.GetValues()
	if err != nil {
		return err
	}
	garbageCollectionRowkey, err := garbageCollection.GetRowkey()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	putRequest, err := hrpc.NewPutStr(ctx, GARBAGE_COLLECTION_TABLE,
		garbageCollectionRowkey, garbageCollectionValues)
	if err != nil {
		return err
	}
	_, err = h.Client.Put(putRequest)
	return err
}

func (h *HbaseClient) ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error) {
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	scanRequest, err := hrpc.NewScanRangeStr(ctx, GARBAGE_COLLECTION_TABLE,
		startRowKey, "",
		// scan for max+1 rows to determine if results are truncated
		hrpc.NumberOfRows(uint32(limit)))

	//scanRequest, err := hrpc.NewScanStr(ctx, GARBAGE_COLLECTION_TABLE,
	//      hrpc.NumberOfRows(uint32(limit)))
	if err != nil {
		return nil, err
	}
	scanResponse, err := h.Client.Scan(scanRequest)
	if err != nil {
		return nil, err
	}
	objectsToRemove := make([]GarbageCollection, 0, limit)
	for _, result := range scanResponse {
		garbage, err := GarbageCollectionFromResponse(result)
		if err != nil {
			return nil, err
		}
		objectsToRemove = append(objectsToRemove, garbage)
	}
	return objectsToRemove, nil
}

func (h *HbaseClient) RemoveGarbageCollection(garbage GarbageCollection) error {
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, GARBAGE_COLLECTION_TABLE,
		garbage.Rowkey, garbage.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = h.Client.Delete(deleteRequest)
	return err
}

//util function
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
			case "mtime":
				garbage.MTime, err = time.Parse(CREATE_TIME_LAYOUT, string(cell.Value))
				if err != nil {
					return
				}
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

func GarbageCollectionFromObject(o *Object) (gc GarbageCollection) {
	gc.BucketName = o.BucketName
	gc.ObjectName = o.Name
	gc.Location = o.Location
	gc.Pool = o.Pool
	gc.ObjectId = o.ObjectId
	gc.Status = "Pending"
	gc.MTime = time.Now().UTC()
	gc.Parts = o.Parts
	gc.TriedTimes = 0
	return
}
