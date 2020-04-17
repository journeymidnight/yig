package tikvclient

import (
	. "database/sql/driver"

	. "github.com/journeymidnight/yig/meta/types"
)

//TODO

//freezer
func (c *TiKVClient) CreateFreezer(freezer *Freezer) (err error) {
	return
}

func (c *TiKVClient) GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error) {
	return
}

func (c *TiKVClient) GetFreezerStatus(bucketName, objectName, version string) (freezer *Freezer, err error) {
	return
}

func (c *TiKVClient) UploadFreezerDate(bucketName, objectName string, lifetime int) (err error) {
	return
}

func (c *TiKVClient) DeleteFreezer(bucketName, objectName string, tx Tx) (err error) {
	return
}
