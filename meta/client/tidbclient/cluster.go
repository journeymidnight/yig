package tidbclient

import (
	"fmt"
	. "github.com/journeymidnight/yig/meta/types"
)

//cluster
func (t *TidbClient) GetCluster(fsid, pool string) (cluster Cluster, err error) {
	sqltext := fmt.Sprintf("select fsid,pool,weight from cluster where fsid='%s' and pool='%s'", fsid, pool)
	err = t.Client.QueryRow(sqltext).Scan(
		&cluster.Fsid,
		&cluster.Pool,
		&cluster.Weight,
	)
	return
}
