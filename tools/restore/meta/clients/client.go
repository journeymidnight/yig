package clients

import (
	. "database/sql/driver"

	"github.com/journeymidnight/yig-restore/meta/common"
	. "github.com/journeymidnight/yig-restore/meta/types"
)

//DB Client Interface
type Client interface {
	//Transaction
	NewTrans() (tx Tx, err error)
	AbortTrans(tx Tx) error
	CommitTrans(tx Tx) error
	//object
	GetObject(bucketName, objectName, version string) (object *Object, err error)
	GetAllObject(bucketName, objectName, version string) (object []*Object, err error)
	//cluster
	GetClusters() (cluster []Cluster, err error)
	//gc
	PutObjectToGarbageCollection(object *Object, tx Tx) error
	PutFreezerToGarbageCollection(object *Freezer, tx Tx) error
	//freezer
	ListFreezers(maxKeys int) (retFreezers []Freezer, err error)
	ListFreezersNeedContinue(maxKeys int, status common.Status) (retFreezers []Freezer, err error)
	GetFreezer(bucketName, objectName, version string) (freezer *Freezer, err error)
	PutFreezer(freezer *Freezer, status common.Status, tx Tx) (err error)
	UploadFreezerStatus(bucketName, objectName, version string, status, statusSetting common.Status) (err error)
	UploadFreezerBackendInfo(targetFreezer *Freezer) error
	DeleteFreezer(bucketName, objectName, version string, objectType ObjectType, createTime uint64, tx Tx) (err error)
}
