package tidbclient

import "database/sql"

func (t *TidbClient) NewTrans() (tx *sql.Tx, err error) {
	tx, err = t.Client.Begin()
	return
}

func (t *TidbClient) AbortTrans(tx *sql.Tx) (err error) {
	err = tx.Rollback()
	return
}

func (t *TidbClient) CommitTrans(tx *sql.Tx) (err error) {
	err = tx.Commit()
	return
}