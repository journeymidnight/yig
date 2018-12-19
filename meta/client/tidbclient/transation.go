package tidbclient

import "database/sql"

func (t *TidbClient) NewTrans()(tx interface{}, err error) {
	tx, err = t.Client.Begin()
	return
}

func (t *TidbClient) AbortTrans(tx interface{}) (err error) {
	err = tx.(* sql.Tx).Rollback()
	return
}

func (t *TidbClient) CommitTrans(tx interface{}) (err error) {
	err = tx.(* sql.Tx).Commit()
	return
}