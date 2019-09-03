package tidbclient

import (
	"database/sql"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/journeymidnight/yig/helper"
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
	conn.SetMaxIdleConns(helper.CONFIG.DbMaxIdleConns)
	conn.SetMaxOpenConns(helper.CONFIG.DbMaxOpenConns)
	conn.SetConnMaxLifetime(time.Duration(helper.CONFIG.DbConnMaxLifeSeconds) * time.Second)
	cli.Client = conn
	return cli
}
