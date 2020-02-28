package meta

import (
	"github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) CreateFreezer(freezer *types.Freezer) error {
	return m.Client.CreateFreezer(freezer)
}

func (m *Meta) GetFreezer(bucketName string, objectName string, version string) (freezer *types.Freezer, err error) {
	return m.Client.GetFreezer(bucketName, objectName, "")
}

func (m *Meta) GetFreezerStatus(bucketName string, objectName string, version string) (freezer *types.Freezer, err error) {
	return m.Client.GetFreezerStatus(bucketName, objectName, "")
}
