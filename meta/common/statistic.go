package common

import "github.com/journeymidnight/yig/helper"

const (
	MinStandardIaObjectSize int64 = 1 << 16 // 64KB
	MinGlacierObjectSize    int64 = 1 << 16 // 64KB
)

// For billing now. FIXME later?
func CorrectDeltaSize(storageClass StorageClass, deltaSize int64) (delta int64) {
	var isNagative bool
	if deltaSize == 0 {
		return delta
	}
	if deltaSize < 0 {
		deltaSize *= -1
		isNagative = true
	}
	if storageClass == ObjectStorageClassStandardIa && deltaSize < MinStandardIaObjectSize {
		deltaSize = MinStandardIaObjectSize
	} else if storageClass == ObjectStorageClassGlacier && deltaSize < MinGlacierObjectSize {
		deltaSize = MinGlacierObjectSize
	}
	delta = helper.Ternary(isNagative, -deltaSize, deltaSize).(int64)
	return
}

type UnexpiredTriple struct {
	StorageClass StorageClass
	Size         int64
	SurvivalTime int64 //Nano seconds
}
