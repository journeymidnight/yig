package tidbclient

import (
	. "github.com/journeymidnight/yig/error"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetAllUserQos() (userQos map[string]UserQos, err error) {
	userQos = make(map[string]UserQos)
	rows, err := t.Client.Query(`select userid, read_qps, write_qps, bandwidth 
		from qos`)
	if err != nil {
		return userQos, NewError(InTidbFatalError, "GetAllUserQos query err", err)
	}
	defer rows.Close()

	for rows.Next() {
		var qos UserQos
		err = rows.Scan(&qos.UserID, &qos.ReadQps, &qos.WriteQps, &qos.Bandwidth)
		if err != nil {
			return userQos, NewError(InTidbFatalError, "GetAllUserQos scan row err", err)
		}
		userQos[qos.UserID] = qos
	}
	return
}
