package tikvclient

import (
	"context"

	. "github.com/journeymidnight/yig/meta/types"
)

func (c *TiKVClient) GetClusters() (cluster []Cluster, err error) {
	startKey := GenKey(TableClusterPrefix)
	endKey := GenKey(TableClusterPrefix, MaxKey)
	c.rawCli.Scan(context.TODO(), startKey, endKey, 1000)
	return
}
