package tidbclient

import (
	"database/sql"
	. "database/sql/driver"
	. "github.com/journeymidnight/yig/error"
)

func (t *TidbClient) NewTrans() (tx Tx, err error) {
	tx, err = t.Client.Begin()
	if err != nil {
		err = NewError(InTidbFatalError, "NewTrans err", err)
	}
	return
}

func (t *TidbClient) AbortTrans(tx Tx) (err error) {
	err = tx.(*sql.Tx).Rollback()
	if err != nil {
		err = NewError(InTidbFatalError, "AbortTrans err", err)
	}
	return
}

func (t *TidbClient) CommitTrans(tx Tx) (err error) {
	err = tx.(*sql.Tx).Commit()
	if err != nil {
		err = NewError(InTidbFatalError, "CommitTrans err", err)
	}
	return
}
