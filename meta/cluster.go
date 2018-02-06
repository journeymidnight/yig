package meta

import (
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

func (m *Meta) GetCluster(fsid string, pool string) (cluster Cluster, err error) {
	rowKey := fsid + ObjectNameSeparator + pool
	getCluster := func() (c interface{}, err error) {
		return m.Client.GetCluster(fsid, pool)
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
	cluster, ok := c.(Cluster)
	if !ok {
		err = ErrInternalError
		return
	}
	return cluster, nil
}
