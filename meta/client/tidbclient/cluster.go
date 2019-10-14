package tidbclient

import (
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetClusters(pool string) (cluster []Cluster, err error) {
	sqltext := "select fsid,pool,weight from cluster where pool=? "
	rows, err := t.Client.Query(sqltext, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		c := Cluster{}
		err = rows.Scan(&c.Fsid, &c.Pool, &c.Weight)
		cluster = append(cluster, c)
		if err != nil {
			return nil, err
		}
	}
	return cluster, nil
}
