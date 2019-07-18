package seaweed

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"io"
)

// Storage implements yig.storage.backend
type Storage struct {
	logger *log.Logger
	seaweedClient *wdclient.MasterClient
}

func NewSeaweedStorage(logger *log.Logger, config helper.Config) Storage {
	clientId := fmt.Sprintf("YIG-%s", string(helper.GenerateRandomId()))
	logger.Logger.Println("Initializing Seaweedfs client:", clientId)
	seaweedClient := wdclient.NewMasterClient(context.Background(),
		nil, clientId, config.SeaweedMasters)
	return Storage{
		logger: logger,
		seaweedClient: seaweedClient,
	}
}

func (s Storage) ClusterID() string {
	return ""
}

func (s Storage) AssignObjectName() string {
	return ""
}

func (s Storage) Put(poolName, objectName string,
	object io.Reader) (bytesWritten uint64, err error) {

	return 0, nil
}

func (s Storage) Append(poolName, objectName string, objectChunk io.Reader,
	offset int64, metaExist bool) (bytesWritten uint64, err error) {

	return 0, nil
}

func (s Storage) GetReader(poolName, objectName string,
	offset int64, length uint64) (reader io.ReadCloser, err error) {

	return nil, nil
}

func (s Storage) Remove(poolName, objectName string) (err error) {
	return nil
}