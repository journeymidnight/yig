package meta

import (
	"github.com/journeymidnight/yig-restore/helper"
	"github.com/journeymidnight/yig-restore/meta/clients"
	"github.com/journeymidnight/yig-restore/meta/clients/tidbclient"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type Meta struct {
	Client clients.Client
}

func New() *Meta {
	meta := Meta{}
	if helper.Conf.DBStore == "tidb" {
		meta.Client = tidbclient.NewTidbClient()
	} else {
		panic("unsupport metastore")
	}
	return &meta
}
