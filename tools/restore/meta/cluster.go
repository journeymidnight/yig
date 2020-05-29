package meta

import (
	. "github.com/journeymidnight/yig-restore/error"
	"github.com/journeymidnight/yig-restore/helper"
	. "github.com/journeymidnight/yig-restore/meta/types"
)

func (m *Meta) GetClusters() (cluster []Cluster, err error) {
	getCluster := func() (c interface{}, err error) {
		helper.Logger.Info("GetClusters CacheMiss")
		return m.Client.GetClusters()
	}
	c, err := getCluster()
	if err != nil {
		return
	}
	cluster, ok := c.([]Cluster)
	if !ok {
		err = ErrInternalError
		return
	}
	return cluster, nil
}
