package meta

import (
	"context"
	"encoding/json"
	"errors"
	. "legitlab.letv.cn/yig/yig/error"
	"legitlab.letv.cn/yig/yig/helper"
	"legitlab.letv.cn/yig/yig/redis"
	"github.com/cannium/gohbase/hrpc"
	"strconv"
)

type Cluster struct {
	Fsid   string
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

func (m *Meta) GetCluster(fsid string) (cluster Cluster, err error) {
	getCluster := func() (c interface{}, err error) {
		ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
		defer done()
		getRequest, err := hrpc.NewGetStr(ctx, CLUSTER_TABLE, fsid)
		if err != nil {
			return
		}
		response, err := m.Hbase.Get(getRequest)
		if err != nil {
			m.Logger.Println("Error getting cluster info, with error", err)
			return
		}
		if len(response.Cells) == 0 {
			err = errors.New("No such cluster")
			return
		}
		var cluster Cluster
		cluster.Fsid = fsid
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
		err := json.Unmarshal(in, &cluster)
		return cluster, err
	}
	c, err := m.Cache.Get(redis.ClusterTable, fsid, getCluster, unmarshaller)
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
