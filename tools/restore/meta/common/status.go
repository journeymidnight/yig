package common

import . "github.com/journeymidnight/yig-restore/error"

type Status uint8

const (
	ObjectNeedRestore Status = iota
	ObjectRestoring
	ObjectHasRestored
)

var (
	StatusIndexMap = map[Status]string{
		ObjectNeedRestore: "READY",
		ObjectRestoring:   "RESTORING",
		ObjectHasRestored: "FINISH",
	}

	StatusStringMap = map[string]Status{
		"READY":     ObjectNeedRestore,
		"RESTORING": ObjectRestoring,
		"FINISH":    ObjectHasRestored,
	}
)

func (s Status) ToString() string {
	return StatusIndexMap[s]
}

func MatchStatusIndex(storageClass string) (Status, error) {
	if index, ok := StatusStringMap[storageClass]; ok {
		return index, nil
	} else {

		return 0, ErrInvalidStatus

	}
}
