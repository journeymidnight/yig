package common

import . "github.com/journeymidnight/yig/error"

type RestoreStatus uint8

const (
	ObjectNeedRestore RestoreStatus = iota
	ObjectRestoring
	ObjectHasRestored
)

var (
	StatusIndexMap = map[RestoreStatus]string{
		ObjectNeedRestore: "READY",
		ObjectRestoring:   "RESTORING",
		ObjectHasRestored: "FINISH",
	}

	StatusStringMap = map[string]RestoreStatus{
		"READY":     ObjectNeedRestore,
		"RESTORING": ObjectRestoring,
		"FINISH":    ObjectHasRestored,
	}
)

func (s RestoreStatus) ToString() string {
	return StatusIndexMap[s]
}

func MatchStatusIndex(status string) (RestoreStatus, error) {
	if index, ok := StatusStringMap[status]; ok {
		return index, nil
	} else {
		return 0, ErrInvalidStatus
	}
}
