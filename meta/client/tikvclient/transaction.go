package tikvclient

import "database/sql"

func (c *TiKVClient) NewTrans() (tx *sql.Tx, err error) { return nil, nil }
func (c *TiKVClient) AbortTrans(tx *sql.Tx) error       { return nil }
func (c *TiKVClient) CommitTrans(tx *sql.Tx) error      { return nil }
