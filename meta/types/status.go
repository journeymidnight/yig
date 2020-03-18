package types

import . "github.com/journeymidnight/yig/error"

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

func MatchStatusIndex(status string) (Status, error) {
	if index, ok := StatusStringMap[status]; ok {
		return index, nil
	} else {
		return 0, ErrInvalidStatus
	}
}
