package tidbclient

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"os"
)

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient() *TidbClient {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", "root:@tcp(10.5.0.9:3306)/yig")
	if err != nil {
		os.Exit(1)
	}
	cli.Client = conn
	return cli
}
