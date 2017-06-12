package meta

import (
	"context"
	"github.com/journeymidnight/yig/helper"
	"github.com/cannium/gohbase/hrpc"
	"bytes"
)

type LifeCycle struct {
	BucketName string
	Status     string // status of this entry, in Pending/Deleting
}

type ScanLifeCycleResult struct {
	Truncated bool
	NextMarker string
	// List of LifeCycles info for this request.
	Lcs []LifeCycle
}

func (lc LifeCycle) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		LIFE_CYCLE_COLUMN_FAMILY: map[string][]byte{
			"status":   []byte(lc.Status),
		},
	}
	return
}

func (lc LifeCycle) GetRowkey() (string, error) {
	return lc.BucketName, nil
}

func LifeCycleFromBucket(b Bucket) (lc LifeCycle) {
	lc.BucketName = b.Name
	lc.Status = "Pending"
	return
}

func (lc LifeCycle) GetValuesForDelete() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		LIFE_CYCLE_COLUMN_FAMILY:      map[string][]byte{},
	}
}

func (m *Meta) PutBucketToLifeCycle(bucket Bucket) error {
	lifeCycle := LifeCycleFromBucket(bucket)

	lifeCycleValues, err := lifeCycle.GetValues()
	if err != nil {
		return err
	}
	lifeCycleRowkey, err := lifeCycle.GetRowkey()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	putRequest, err := hrpc.NewPutStr(ctx, LIFE_CYCLE_TABLE,
		lifeCycleRowkey, lifeCycleValues)
	if err != nil {
		return err
	}
	_, err = m.Hbase.Put(putRequest)
	return err
}

func (m *Meta) RemoveBucketFromLifeCycle(bucket Bucket) error {
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, LIFE_CYCLE_TABLE,
		bucket.Name, map[string]map[string][]byte{})
	if err != nil {
		return err
	}
	_, err = m.Hbase.Delete(deleteRequest)
	return err
}

func LifeCycleFromResponse(response *hrpc.Result) (lc LifeCycle, err error) {
	lc = LifeCycle{}
	for _, cell := range response.Cells {
		lc.BucketName = string(cell.Row)

		switch string(cell.Family) {
		case LIFE_CYCLE_COLUMN_FAMILY:
			switch string(cell.Qualifier) {
			case "status":
				lc.Status = string(cell.Value)
			}
		}

	}
	return lc, nil
}

func (m *Meta) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	var startKey bytes.Buffer
	var stopKey bytes.Buffer
	result.Truncated = false
	if marker != "" {
		startKey.WriteString(marker)
	}
	scanRequest, err := hrpc.NewScanRangeStr(ctx, LIFE_CYCLE_TABLE,
		startKey.String(), stopKey.String(),
		// scan for max+2 rows to determine if results are truncated
		hrpc.NumberOfRows(uint32(limit+2)))
	if err != nil {
		return
	}
	scanResponse, err := m.Hbase.Scan(scanRequest)
	if err != nil {
		return
	}

	if len(scanResponse) > 0 {
		firstBucket, err := LifeCycleFromResponse(scanResponse[0])
		if err != nil {
			return result, err
		}

		if marker == "" || (marker != "" && marker != firstBucket.BucketName) {
			if len(scanResponse) > limit {
				result.Truncated = true
				var nextBucket LifeCycle
				nextBucket, err = LifeCycleFromResponse(scanResponse[limit-1])
				if err != nil {
					return result, err
				}
				result.NextMarker = nextBucket.BucketName
				scanResponse = scanResponse[:limit]
			}
		} else if marker != "" && marker == firstBucket.BucketName {
			if len(scanResponse) > (limit + 1) {
				result.Truncated = true
				var nextBucket LifeCycle
				nextBucket, err = LifeCycleFromResponse(scanResponse[limit])
				if err != nil {
					return result, err
				}
				result.NextMarker = nextBucket.BucketName
				scanResponse = scanResponse[1 : limit+1]
			} else {
				scanResponse = scanResponse[1:(len(scanResponse))]
			}
		}
	}
	result.Lcs = make([]LifeCycle, 0, limit)
	for _, Response := range scanResponse {
		lc, err := LifeCycleFromResponse(Response)
		if err != nil {
			return result, err
		}
		result.Lcs = append(result.Lcs, lc)
	}
	return result, nil
}