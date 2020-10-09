package tidbclient

import (
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetClusters() (cluster []Cluster, err error) {
	sqltext := "select fsid,pool,weight from cluster"
	rows, err := t.Client.Query(sqltext)
	if err != nil {
		return nil, NewError(InTidbFatalError, "GetClusters query err", err)
	}
	defer rows.Close()
	for rows.Next() {
		c := Cluster{}
		err = rows.Scan(&c.Fsid, &c.Pool, &c.Weight)
		cluster = append(cluster, c)
		if err != nil {
			return nil, NewError(InTidbFatalError, "GetClusters scan row err", err)
		}
	}
	return cluster, nil
}
