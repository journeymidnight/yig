package types

import "database/sql"

// This should work with database/sql.DB and database/sql.Tx.
// Stolen from xo/xo
type DB interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
}
