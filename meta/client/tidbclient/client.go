package tidbclient

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/journeymidnight/yig/helper"
	"os"
)

const MAX_OPEN_CONNS = 1024

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient() *TidbClient {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", helper.CONFIG.TidbInfo)
	if err != nil {
		os.Exit(1)
	}
	conn.SetMaxIdleConns(0)
	conn.SetMaxOpenConns(MAX_OPEN_CONNS)
	cli.Client = conn
	return cli
}
