package helper

import (
	"os"
	"encoding/json"
	"errors"
)


type GcCfg struct {
	S3Domain string //YIG域名
	IamEndpoint string //IAM提供的注册服务地址
	IamKey string //IAM提供的注册服务地址
	IamSecret string //IAM提供的注册服务地址
	LogPath string
	PanicLogPath string
	PidFile string
	BindApiAddress string
	BindAdminAddress string
	SSLKeyPath string
	SSLCertPath string
	ZookeeperAddress string
}

var Cfg *GcCfg

func GetGcCfg() (cfg GcCfg, err error) {
	// TODO(wenjianhn): get json from etcd

	f, err := os.Open("/etc/yig/yig.json")
	if err != nil {
		Logger.Println("Parse adapter.json failed")
		return
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&cfg)
	if err != nil {
		err = errors.New("failed to parse adapter.json: " + err.Error())
		Logger.Println("Parse adapter.json failed")
	}

	return
}