package meta

import (
	"context"
	"fmt"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
)

const (
	CLUSTER_CACHE_PREFIX = "cluster:"
)

func (m *Meta) GetCluster(ctx context.Context, fsid string, pool string) (cluster Cluster, err error) {
	rowKey := fsid + ObjectNameEnding + pool
	getCluster := func() (c helper.Serializable, err error) {
		helper.Logger.Println(10, "[", helper.RequestIdFromContext(ctx), "]", "GetCluster CacheMiss. fsid:", fsid)
		cl, err := m.Client.GetCluster(fsid, pool)
		c = &cl
		return c, err
	}

	toCluster := func(fields map[string]string) (interface{}, error) {
		c := &Cluster{}
		return c.Deserialize(fields)
	}

	c, err := m.Cache.Get(ctx, redis.ClusterTable, CLUSTER_CACHE_PREFIX, rowKey, getCluster, toCluster, true)
	if err != nil {
		helper.Logger.Println(20, fmt.Sprintf("failed to get cluster for fsid: %s, err: %v", fsid, err))
		return
	}
	cluster, ok := c.(Cluster)
	if !ok {
		err = ErrInternalError
		return
	}
	return cluster, nil
}
