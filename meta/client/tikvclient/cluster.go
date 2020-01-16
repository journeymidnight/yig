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
	startKey := GenKey(TableClusterPrefix, TableMinKeySuffix)
	endKey := GenKey(TableClusterPrefix, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, MaxClusterKeyLimit)
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		cluster, err := getCluster(kv.K, kv.V)
		if err != nil {
			helper.Logger.Warn("get cluster err:", err)
			continue
		}
		clusters = append(clusters, cluster)
	}
	return
}

// Key: c\{PoolName}\{Fsid}\{Backend}
func getCluster(k, v []byte) (c Cluster, err error) {
	kStr := string(k)
	sp := strings.Split(kStr, TableSeparator)
	if len(sp) != 4 {
		return c, errors.New("invalid cluster key:" + kStr)
	}
	var w int
	err = helper.MsgPackUnMarshal(v, &w)
	if err != nil {
		return c, err
	}
	c.Pool = sp[1]
	c.Fsid = sp[2]
	backend, err := strconv.Atoi(sp[3])
	if err != nil {
		return c, err
	}
	c.Backend = BackendType(backend)
	c.Weight = w
	return
}
