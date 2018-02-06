package hbaseclient

import (
	"context"
	"github.com/cannium/gohbase/hrpc"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"strconv"
)

func (h *HbaseClient) GetCluster(fsid, pool string) (cluster Cluster, err error) {
	rowKey := fsid + ObjectNameSeparator + pool
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	getRequest, err := hrpc.NewGetStr(ctx, CLUSTER_TABLE, rowKey)
	if err != nil {
		return
	}
	response, err := h.Client.Get(getRequest)
	if err != nil {
		return
	}
	if len(response.Cells) == 0 {
		return cluster, nil
	}
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
