package tikvclient

import (
	"context"

	"github.com/journeymidnight/client-go/key"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// **Key**: l\{BucketName}
func genLifecycleKey(bucketName string) []byte {
	return GenKey(TableLifeCyclePrefix, bucketName)
}

//lc
func (c *TiKVClient) PutBucketToLifeCycle(bucket Bucket, lifeCycle LifeCycle) error {
	bucketKey := genBucketKey(bucket.Name)
	lcKey := genLifecycleKey(bucket.Name)
	return c.TxPut(bucketKey, bucket, lcKey, lifeCycle)
}

func (c *TiKVClient) GetBucketLifeCycle(bucket Bucket) (*LifeCycle, error) {
	key := genLifecycleKey(bucket.Name)
	var lc LifeCycle
	ok, err := c.TxGet(key, &lc, nil)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &lc, nil
}

func (c *TiKVClient) RemoveBucketFromLifeCycle(bucket Bucket) (err error) {
	bucketKey := genBucketKey(bucket.Name)
	lcKey := genLifecycleKey(bucket.Name)
	tx, err := c.NewTrans()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = c.CommitTrans(tx)
		}
		if err != nil {
			c.AbortTrans(tx)
		}
	}()

	txn := tx.(*TikvTx).tx

	bucketVal, err := helper.MsgPackMarshal(bucket)
	if err != nil {
		return err
	}

	err = txn.Set(bucketKey, bucketVal)
	if err != nil {
		return err
	}

	err = txn.Delete(lcKey)
	if err != nil {
		return err
	}

	return nil
}

func (c *TiKVClient) ScanLifeCycle(limit int, marker string) (result ScanLifeCycleResult, err error) {
	startKey := genLifecycleKey(marker)
	endKey := genLifecycleKey(TableMaxKeySuffix)

	tx, err := c.TxnCli.Begin(context.TODO())
	if err != nil {
		return result, err
	}
	it, err := tx.Iter(context.TODO(), key.Key(startKey), key.Key(endKey))
	if err != nil {
		return result, err
	}
	defer it.Close()
	for it.Valid() {
		v := it.Value()
		var lc LifeCycle
		err = helper.MsgPackUnMarshal(v, &lc)
		if err != nil {
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
			return result, err
		}
	}
	return
}
