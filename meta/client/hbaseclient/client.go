package hbaseclient

import (
	"context"
	"github.com/cannium/gohbase"
	"github.com/journeymidnight/yig/helper"
)

var RootContext = context.Background()

type HbaseClient struct {
	Client gohbase.Client
}

func NewHbaseClient() *HbaseClient {
	cli := &HbaseClient{}
	znodeOption := gohbase.SetZnodeParentOption(helper.CONFIG.HbaseZnodeParent)
	cli.Client = gohbase.NewClient(helper.CONFIG.ZookeeperAddress, znodeOption)

	return cli
}
