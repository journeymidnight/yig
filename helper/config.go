package helper

import (
	"encoding/json"
	"os"
)

type Config struct {
	S3Domain                   string // Domain name of YIG
	Region                     string // Region name this instance belongs to, e.g cn-bj-1
	IamEndpoint                string // le IAM endpoint address
	IamKey                     string
	IamSecret                  string
	LogPath                    string
	PanicLogPath               string
	PidFile                    string
	BindApiAddress             string
	BindAdminAddress           string
	SSLKeyPath                 string
	SSLCertPath                string
	ZookeeperAddress           string
	RedisAddress               string // redis connection string, e.g localhost:1234
	RedisConnectionNumber      int    // number of connections to redis(i.e max concurrent request number)
	InMemoryCacheMaxEntryCount int
	InstanceId                 string // if empty, generated one at server startup
	ConcurrentRequestLimit     int
	HbaseZnodeParent           string // won't change default("/hbase") if leave this option empty
}

var CONFIG Config

func SetupConfig() {
	f, err := os.Open("/etc/yig/yig.json")
	if err != nil {
		panic("Cannot open yig.json")
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&CONFIG)
	if err != nil {
		panic("Failed to parse yig.json: " + err.Error())
	}
}
