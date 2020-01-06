package tikvclient

import (
	"errors"
	"strconv"
	"strings"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

const (
	MaxClusterKeyLimit = 1000
)

//cluster
func (c *TiKVClient) GetClusters() (clusters []Cluster, err error) {
	startKey := GenKey(true, TableClusterPrefix)
	endKey := GenKey(false, TableClusterPrefix, string(TableMaxKeySuffix))
	kvs, err := c.Scan(startKey, endKey, MaxClusterKeyLimit)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		kStr := string(kv.K)
		vStr := string(kv.V)
		cluster, err := getCluster(kStr, vStr)
		if err != nil {
			helper.Logger.Warn("get cluster err:", err)
			continue
		}
		clusters = append(clusters, cluster)
	}
	return
}

// Key: c\{PoolName}\{Fsid}\{Backend}
func getCluster(k, v string) (c Cluster, err error) {
	sp := strings.Split(k, string(TableSeparator))
	if len(sp) != 4 {
		return c, errors.New("invalid cluster key:" + k)
	}
	w, err := strconv.Atoi(v)
	if err != nil {
		return c, err
	}
	c.Pool = sp[1]
	c.Fsid = sp[2]
	c.Backend = sp[3]
	c.Weight = w
	return
}
