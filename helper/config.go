package helper

import (
	"encoding/json"
	"os"
	"time"
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
	HbaseTimeout               time.Duration
	DebugMode                  bool
	AdminKey                   string
	GcThread                   int
	MetaCacheType              int
	EnableDataCache            bool
	LcThread                   int
	LcDebug                    bool   //if this was set true, lc expiration will treat days as seconds for test
}

type config struct {
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
	HbaseTimeout               int    // in seconds
	DebugMode                  bool
	AdminKey                   string
	GcThread                   int
	MetaCacheType              int
	EnableDataCache            bool
	LcThread                   int
	LcDebug                    bool   //if this was set true, lc expiration will treat days as seconds for test
}

var CONFIG Config

func SetupConfig() {
	f, err := os.Open("/etc/yig/yig.json")
	if err != nil {
		panic("Cannot open yig.json")
	}
	defer f.Close()

	var c config
	err = json.NewDecoder(f).Decode(&c)
	if err != nil {
		panic("Failed to parse yig.json: " + err.Error())
	}

	// setup CONFIG with defaults
	CONFIG.S3Domain = c.S3Domain
	CONFIG.Region = c.Region
	CONFIG.IamEndpoint = c.IamEndpoint
	CONFIG.IamKey = c.IamKey
	CONFIG.IamSecret = c.IamSecret
	CONFIG.LogPath = c.LogPath
	CONFIG.PanicLogPath = c.PanicLogPath
	CONFIG.PidFile = c.PidFile
	CONFIG.BindApiAddress = c.BindApiAddress
	CONFIG.BindAdminAddress = c.BindAdminAddress
	CONFIG.SSLKeyPath = c.SSLKeyPath
	CONFIG.SSLCertPath = c.SSLCertPath
	CONFIG.EnableDataCache = c.EnableDataCache
	CONFIG.MetaCacheType = c.MetaCacheType
	CONFIG.ZookeeperAddress = c.ZookeeperAddress
	CONFIG.RedisAddress = c.RedisAddress
	CONFIG.RedisConnectionNumber = Ternary(c.RedisConnectionNumber == 0,
		10, c.RedisConnectionNumber).(int)
	CONFIG.InMemoryCacheMaxEntryCount = Ternary(c.InMemoryCacheMaxEntryCount == 0,
		100000, c.InMemoryCacheMaxEntryCount).(int)
	CONFIG.InstanceId = Ternary(c.InstanceId == "",
		string(GenerateRandomId()), c.InstanceId).(string)
	CONFIG.ConcurrentRequestLimit = Ternary(c.ConcurrentRequestLimit == 0,
		10000, c.ConcurrentRequestLimit).(int)
	CONFIG.HbaseZnodeParent = Ternary(c.HbaseZnodeParent == "",
		"/hbase", c.HbaseZnodeParent).(string)
	CONFIG.HbaseTimeout = Ternary(c.HbaseTimeout == 0, 30*time.Second,
		time.Duration(c.HbaseTimeout)*time.Second).(time.Duration)
	CONFIG.DebugMode = c.DebugMode
	CONFIG.AdminKey = c.AdminKey
	CONFIG.GcThread = Ternary(c.GcThread == 0,
		1, c.GcThread).(int)
	CONFIG.LcThread = Ternary(c.LcThread == 0,
		1, c.LcThread).(int)
	CONFIG.LcDebug = c.LcDebug
}
