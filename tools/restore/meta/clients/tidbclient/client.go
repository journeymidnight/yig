package tidbclient

import (
	"database/sql"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/journeymidnight/yig-restore/helper"
)

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient() *TidbClient {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", helper.Conf.TidbInfo)
	if err != nil {
		os.Exit(1)
	}
	conn.SetMaxIdleConns(helper.Conf.DbMaxIdleConns)
	conn.SetMaxOpenConns(helper.Conf.DbMaxOpenConns)
	conn.SetConnMaxLifetime(time.Duration(helper.Conf.DbConnMaxLifeSeconds) * time.Second)
	cli.Client = conn
	return cli
}
