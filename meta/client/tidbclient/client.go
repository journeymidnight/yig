package tidbclient

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/journeymidnight/yig/helper"
	"os"
)

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient() *TidbClient {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", helper.CONFIG.TidbInfo)
	if err != nil {
		os.Exit(1)
	}
	cli.Client = conn
	return cli
}
