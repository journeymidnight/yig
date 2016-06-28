package storage

import (
	"git.letv.cn/zhangdongmao/radoshttpd/rados"
)

type YigStorage struct {
	// *YigStorage implements minio.ObjectLayer
	Rados *rados.Conn
	// TODO
}
