/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"git.letv.cn/zhangdongmao/radoshttpd/rados"
	"log"
	"os"
	"git.letv.cn/yig/yig/storage"
	"git.letv.cn/yig/yig/minio"
)

// TODO config file
var (
	LOGPATH                                = "/var/log/yig/yig.log"
	PANIC_LOG_PATH			       = "/var/log/yig/panic.log"
	PIDFILE                                = "/var/run/yig/yig.pid"
	CEPH_CONFIG_PATH		       = "./conf/ceph.conf"
	MONTIMEOUT                             = "10"
	OSDTIMEOUT                             = "10"
	BUFFERSIZE                             = 4 << 20 /* 4M */
	AIOCONCURRENT                          = 4
	MAX_CHUNK_SIZE                         = BUFFERSIZE * 2
	STRIPE_UNIT                            = uint(512 << 10) /* 512K */
	OBJECT_SIZE                            = uint(4 << 20)   /* 4M */
	STRIPE_COUNT                           = uint(4)
	CONCURRENT_REQUEST_LIMIT               = 100 // 0 for "no limit"
	BIND_ADDRESS			       = "0.0.0.0:3000"
	HOST_URL			       = "127.0.0.1" /* should be something like
								s3.lecloud.com
								for production servers
							     */
	SSL_KEY_PATH		       	      = ""
	SSL_CERT_PATH			      = ""
	REGION				      = "cn-bj-1"

	/* global variables */
	logger                    *log.Logger
	yig			  *storage.YigStorage
)

func set_stripe_layout(p *rados.StriperPool) int {
	var ret int = 0
	if ret = p.SetLayoutStripeUnit(STRIPE_UNIT); ret < 0 {
		return ret
	}
	if ret = p.SetLayoutObjectSize(OBJECT_SIZE); ret < 0 {
		return ret
	}
	if ret = p.SetLayoutStripeCount(STRIPE_COUNT); ret < 0 {
		return ret
	}
	return ret
}

func main() {
	f, err := os.OpenFile(LOGPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + LOGPATH)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags)

	// you must have admin keyring
	Rados, err := rados.NewConn("admin")
	if err != nil {
		panic("failed to open keyring")
	}

	Rados.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = Rados.ReadConfigFile(CEPH_CONFIG_PATH)
	if err != nil {
		panic("failed to open ceph.conf")
	}

	err = Rados.Connect()
	if err != nil {
		panic("failed to connect to remote cluster")
	}
	defer Rados.Shutdown()

	yig = &storage.YigStorage{
		Rados: Rados,
	}
	apiServerConfig := &minio.ServerConfig{
		Address:BIND_ADDRESS,
		KeyFilePath: SSL_KEY_PATH,
		CertFilePath:SSL_CERT_PATH,
		Region:REGION,
		Logger:logger,
		ObjectLayer:yig,
		MaxConcurrentRequest:CONCURRENT_REQUEST_LIMIT,
	}
	minio.StartApiServer(apiServerConfig)
}

