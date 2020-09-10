package tikvclient

import (
	"math"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: q\{UserId}
func GenQoSKey(userId string) []byte {
	return GenKey(TableQoSPrefix, userId)
}

//qos
func (c *TiKVClient) GetAllUserQos() (userQos map[string]UserQos, err error) {
	userQos = make(map[string]UserQos)
	startKey := GenKey(TableQoSPrefix, TableMinKeySuffix)
	endKey := GenKey(TableQoSPrefix, TableMaxKeySuffix)
	kvs, err := c.TxScan(startKey, endKey, math.MaxInt64, nil)
	for _, kv := range kvs {
		var qos UserQos
		err = helper.MsgPackUnMarshal(kv.V, &qos)
		if err != nil {
			return nil, err
		}
		userQos[qos.UserID] = qos
	}
	return userQos, nil
}
