package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
)

func (t *TidbClient) NewTrans() (tx Tx, err error) {
	tx, err = t.Client.Begin()
	return
}

func (t *TidbClient) AbortTrans(tx Tx) (err error) {
	err = tx.(*sql.Tx).Rollback()
	return
}

func (t *TidbClient) CommitTrans(tx Tx) (err error) {
	err = tx.(*sql.Tx).Commit()
	return
}
