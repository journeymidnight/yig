package tikvclient

import (
	"context"
	"fmt"

	"github.com/journeymidnight/yig/helper"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
)

const (
	TableClusterPrefix    = "c"
	TableBucketPrefix     = "b"
	TableUserBucketPrefix = "u"
	TableMultipartPrefix  = "m"
	TableObjectPartPrefix = "p"
	TableLifeCyclePrefix  = "l"
	TableGcPrefix         = "g"
	TableFreezerPrefix    = "f"
)

var (
	TableMinKeySuffix = ""
	TableMaxKeySuffix = string(0xFF)
	TableSeparator    = string(92) // "\"
)

type TiKVClient struct {
	TxnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient() *TiKVClient {
	TxnCli, err := txnkv.NewClient(context.TODO(), helper.CONFIG.PdAddress, config.Default())
	if err != nil {
		panic(err)
	}
	return &TiKVClient{TxnCli}
}

func (c *TiKVClient) TxGet(k []byte, ref interface{}) (bool, error) {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return false, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil && !kv.IsErrNotFound(err) {
		return false, err
	}
	if kv.IsErrNotFound(err) {
		return false, nil
	}
	return true, helper.MsgPackUnMarshal(v, ref)
}

func (c *TiKVClient) TxExist(k []byte) (bool, error) {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return false, err
	}
	_, err = tx.Get(context.TODO(), k)
	if err != nil && !kv.IsErrNotFound(err) {
		return false, err
	}
	if kv.IsErrNotFound(err) {
		return false, nil
	}
	return true, nil
}

func (c *TiKVClient) TxPut(args ...interface{}) error {
	if len(args)%2 != 0 {
		return fmt.Errorf("tikv txn put need parameters of two or multiples of two.")
	}
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	for i := 0; i < len(args); i += 2 {
		rowKey := args[i].([]byte)
		val := args[i+1]
		v, err := helper.MsgPackMarshal(val)
		if err != nil {
			return err
		}

		err = tx.Set(rowKey, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) TxDelete(keys ...[]byte) error {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) TxScan(keyPrefix []byte, upperBound []byte, limit int) ([]KV, error) {
	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(keyPrefix), key.Key(upperBound))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next(context.TODO())
	}
	return ret, nil
}
