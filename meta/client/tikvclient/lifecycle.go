package tikvclient

import (
	"context"

	"github.com/journeymidnight/client-go/key"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: l\{BucketName}
func GenLifecycleKey(bucketName string) []byte {
	return GenKey(TableLifeCyclePrefix, bucketName)
}

//lc
func (c *TiKVClient) PutBucketToLifeCycle(bucket Bucket, lifeCycle LifeCycle) error {
	bucketKey := GenBucketKey(bucket.Name)
	lcKey := GenLifecycleKey(bucket.Name)
	err := c.TxPut(bucketKey, bucket, lcKey, lifeCycle)
	if err != nil {
		return NewError(InTikvFatalError, "PutBucketToLifeCycle TxPut err", err)
	}
	return nil
}

func (c *TiKVClient) GetBucketLifeCycle(bucket Bucket) (*LifeCycle, error) {
	key := GenLifecycleKey(bucket.Name)
	var lc LifeCycle
	ok, err := c.TxGet(key, &lc, nil)
	if err != nil {
		err = NewError(InTikvFatalError, "GetBucketLifeCycle TxGet err", err)
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &lc, nil
}

func (c *TiKVClient) RemoveBucketFromLifeCycle(bucket Bucket) (err error) {
	bucketKey := GenBucketKey(bucket.Name)
	lcKey := GenLifecycleKey(bucket.Name)
	tx, err := c.NewTrans()
	if err != nil {
		return NewError(InTikvFatalError, "RemoveBucketFromLifeCycle NewTrans err", err)
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
		}
		if err != nil {
			err = NewError(InTikvFatalError, "RemoveBucketFromLifeCycle err", err)
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx

	bucketVal, err := helper.MsgPackMarshal(bucket)
	if err != nil {
		return NewError(InTikvFatalError, "RemoveBucketFromLifeCycle MsgPackMarshal err", err)
	}

	err = txn.Set(bucketKey, bucketVal)
	if err != nil {
		return NewError(InTikvFatalError, "RemoveBucketFromLifeCycle Set err", err)
	}

	err = txn.Delete(lcKey)
	if err != nil {
		return NewError(InTikvFatalError, "RemoveBucketFromLifeCycle Delete err", err)
	}

	return nil
}

func (c *TiKVClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	startKey := GenLifecycleKey(marker)
	endKey := GenLifecycleKey(TableMaxKeySuffix)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		err = NewError(InTikvFatalError, "ScanLifeCycle TCBegin err", err)
		return result, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		err = NewError(InTikvFatalError, "ScanLifeCycle Iter err", err)
		return result, err
	}
	defer it.Close()
	for it.Valid() {
		v := it.Value()
		var lc LifeCycle
		err = helper.MsgPackUnMarshal(v, &lc)
		if err != nil {
			err = NewError(InTikvFatalError, "ScanLifeCycle MsgPackUnMarshal err", err)
			return result, err
		}
		result.Lcs = append(result.Lcs, lc)
		limit--
		if limit == 0 {
			result.NextMarker = lc.BucketName
			result.Truncated = true
			break
		}
		if err := it.Next(context.TODO()); err != nil && it.Valid() {
			err = NewError(InTikvFatalError, "ScanLifeCycle get next err", err)
			return result, err
		}
	}
	return
}
