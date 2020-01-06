package tikvclient

import (
	"context"

	"github.com/journeymidnight/yig/helper"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
)

type TiKVClient struct {
	rawCli *rawkv.Client
	txnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient() TiKVClient {
	rawCli, err := rawkv.NewClient(context.TODO(), []string{helper.CONFIG.PdAddress}, config.Default())
	if err != nil {
		panic(err)
	}

	txnCli, err := txnkv.NewClient(context.TODO(), []string{helper.CONFIG.PdAddress}, config.Default())
	if err != nil {
		panic(err)
	}
	return TiKVClient{rawCli, txnCli}
}

func (c *TiKVClient) Put(k []byte, v interface{}) error {
	b, err := helper.MsgPackMarshal(v)
	if err != nil {
		return err
	}
	return c.rawCli.Put(context.TODO(), k, b)
}

func (c *TiKVClient) Get(k []byte) ([]byte, error) {
	return c.rawCli.Get(context.TODO(), k)
}
