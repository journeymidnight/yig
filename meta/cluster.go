package meta

import (
	"context"
	"strconv"

	"github.com/cannium/gohbase/hrpc"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
)

type Cluster struct {
	Fsid   string
	Pool   string
	Weight int
}

func (c Cluster) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		CLUSTER_COLUMN_FAMILY: map[string][]byte{
			"weight": []byte(strconv.Itoa(c.Weight)),
		},
	}
	return
}

func (m *Meta) GetCluster(fsid string, pool string) (cluster Cluster, err error) {
	rowKey := fsid + ObjectNameSeparator + pool
	getCluster := func() (c interface{}, err error) {
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		getRequest, err := hrpc.NewGetStr(ctx, CLUSTER_TABLE, rowKey)
		if err != nil {
			return
		}
		response, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println(5, "Error getting cluster info, with error", err)
			return
		}
		if len(response.Cells) == 0 {
			return cluster, nil
		}
		var cluster Cluster
		cluster.Fsid = fsid
		cluster.Pool = pool
		for _, cell := range response.Cells {
			switch string(cell.Qualifier) {
			case "weight":
				cluster.Weight, err = strconv.Atoi(string(cell.Value))
				if err != nil {
					return
				}
			}
		}
		return cluster, nil
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
