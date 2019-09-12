package meta

import (
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

func (m *Meta) GetClusters() (cluster []Cluster, err error) {
	rowKey := "cephClusters"
	getCluster := func() (c interface{}, err error) {
		helper.Logger.Println(10, "GetClusters CacheMiss")
		return m.Client.GetClusters()
	}
	unmarshaller := func(in []byte) (interface{}, error) {
		var cluster Cluster
		err := helper.MsgPackUnMarshal(in, &cluster)
		return cluster, err
	}
	c, err := m.Cache.Get(redis.ClusterTable, rowKey, getCluster, unmarshaller, true)
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
