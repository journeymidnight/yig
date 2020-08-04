package tikvclient

import (
	"context"
	"fmt"

	"github.com/journeymidnight/client-go/config"
	"github.com/journeymidnight/client-go/key"
	"github.com/journeymidnight/client-go/txnkv"
	"github.com/journeymidnight/client-go/txnkv/kv"
	"github.com/journeymidnight/yig/helper"
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
	TableHotObjectPrefix  = "h"
	TableQoSPrefix        = "q"
)

var (
	TableMinKeySuffix = ""
	TableMaxKeySuffix = string(0xFF)
	TableSeparator    = string(0x1F)
)

type TiKVClient struct {
	TxnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient(pdAddress []string) *TiKVClient {
	conf := config.Default()
	conf.RPC.MaxConnectionCount = uint(helper.Ternary(helper.CONFIG.DbMaxOpenConns <= 0, 16, helper.CONFIG.DbMaxOpenConns).(int))
	TxnCli, err := txnkv.NewClient(context.TODO(), pdAddress, conf)
	if err != nil {
		panic(err)
	}
	return &TiKVClient{TxnCli}
}

func (c *TiKVClient) TxGet(k []byte, ref interface{}, tx *txnkv.Transaction) (bool, error) {
	var err error
	if tx == nil {
		tx, err = c.TxnCli.Begin(context.TODO())
		if err != nil {
			return false, err
		}
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

func (c *TiKVClient) TxGetPure(k []byte, tx *txnkv.Transaction) ([]byte, error) {
	var err error
	if tx == nil {
		tx, err = c.TxnCli.Begin(context.TODO())
		if err != nil {
			return nil, err
		}
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil && !kv.IsErrNotFound(err) {
		return nil, err
	}
	if kv.IsErrNotFound(err) {
		return nil, nil
	}
	return v, nil
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

func (c *TiKVClient) TxScan(keyPrefix []byte, upperBound []byte, limit int, tx *txnkv.Transaction) ([]KV, error) {
	var err error
	if tx == nil {
		tx, err = c.TxnCli.Begin(context.TODO())
		if err != nil {
			return nil, err
		}
	}
	it, err := tx.Iter(context.TODO(), key.Key(keyPrefix), key.Key(upperBound))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key(), V: it.Value()})
		limit--
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			return nil, err
		}
	}
	return ret, nil
}

func (c *TiKVClient) TxScanCallback(keyPrefix []byte, upperBound []byte,
	tx *txnkv.Transaction, f func(k, v []byte) error) error {

	var err error
	if tx == nil {
		tx, err = c.TxnCli.Begin(context.TODO())
		if err != nil {
			return err
		}
		defer tx.Commit(context.TODO())
	}
	it, err := tx.Iter(context.TODO(), keyPrefix, upperBound)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() {
		err = f(it.Key(), it.Value())
		if err != nil {
			return err
		}
		err = it.Next(context.TODO())
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TiKVClient) TxDeleteRange(keyPrefix []byte, upperBound []byte, limit int, tx *txnkv.Transaction) (int, error) {
	var err error
	var count int
	if tx == nil {
		tx, err = c.TxnCli.Begin(context.TODO())
		if err != nil {
			return 0, err
		}
	}
	defer func() {
		if err == nil {
			err = tx.Commit(context.Background())
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	it, err := tx.Iter(context.TODO(), key.Key(keyPrefix), key.Key(upperBound))
	if err != nil {
		return 0, err
	}
	defer it.Close()
	for it.Valid() && limit > 0 {
		err := tx.Delete(it.Key())
		if err != nil {
			return 0, err
		}
		count++
		limit--
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			return 0, err
		}
	}
	return count, nil
}
