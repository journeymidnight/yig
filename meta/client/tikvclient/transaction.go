package tikvclient

import (
	"context"
	. "database/sql/driver"

	"github.com/tikv/client-go/txnkv"
)

type TikvTx struct {
	tx *txnkv.Transaction
}

func (t *TikvTx) Commit() error {
	return t.tx.Commit(context.TODO())
}

func (t *TikvTx) Rollback() error {
	return t.tx.Rollback()
}

func (c *TiKVClient) NewTrans() (tx Tx, err error) {
	t, err := c.TxnCli.Begin(context.Background())
	if err != nil {
		return nil, err
	}
	return &TikvTx{t}, nil
}

func (c *TiKVClient) AbortTrans(tx Tx) error {
	return tx.(*TikvTx).Rollback()
}

func (c *TiKVClient) CommitTrans(tx Tx) error {
	return tx.(*TikvTx).Commit()
}
