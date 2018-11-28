package helper

import (
	"github.com/BurntSushi/toml"
	"time"
	"io/ioutil"
)

const (
	YIG_CONF_PATH = "/etc/yig/yig.toml"
)

type Config struct {
	S3Domain                   string `toml:"s3domain"` // Domain name of YIG
	Region                     string `toml:"region"` // Region name this instance belongs to, e.g cn-bj-1
	IamEndpoint                string `toml:"iam_endpoint"` // le IAM endpoint address
	IamKey                     string `toml:"iam_key"`
	IamSecret                  string `toml:"iam_secret"`
	LogPath                    string `toml:"log_path"`
	PanicLogPath               string `toml:"panic_log_path"`
	PidFile                    string `toml:"pid_file"`
	BindApiAddress             string `toml:"api_listener"`
	BindAdminAddress           string `toml:"admin_listener"`
	SSLKeyPath                 string `toml:"ssl_key_path"`
	SSLCertPath                string `toml:"ssl_cert_path"`
	ZookeeperAddress           string `toml:"zk_address"`
	RedisAddress               string `toml:"redis_address"` // redis connection string, e.g localhost:1234
	RedisConnectionNumber      int    `toml:"redis_connection_number"` // number of connections to redis(i.e max concurrent request number)
	RedisPassword              string  // redis auth password
	InMemoryCacheMaxEntryCount int    `toml:"memory_cache_max_entry_count"`
	InstanceId                 string  // if empty, generated one at server startup
	ConcurrentRequestLimit     int
	HbaseZnodeParent           string  // won't change default("/hbase") if leave this option empty
	HbaseTimeout               time.Duration     // in seconds
	DebugMode                  bool   `toml:"debug_mode"`
	AdminKey                   string `toml:"admin_key"` //used for tools/admin to communicate with yig
	GcThread                   int
	MetaCacheType              int    `toml:"meta_cache_type"`
	EnableDataCache            bool   `toml:"enable_data_cache"`
	LcThread                   int     //used for tools/lc only, set worker numbers to do lc
	LcDebug                    bool    //used for tools/lc only, if this was set true, will treat days as seconds
	LogLevel                   int    `toml:"log_level"` //1-20
	CephConfigPattern          string `toml:"ceph_config_pattern"`
	ReservedOrigins            string  // www.ccc.com,www.bbb.com,127.0.0.1
	MetaStore                  string `toml:"meta_store"`
	TidbInfo                   string `toml:"tidb_info"`
	KeepAlive                  bool   `toml:"keepalive"`
}

var CONFIG Config

func SetupConfig() {
	MarshalTOMLConfig()
}

func MarshalTOMLConfig() error {
	data, err := ioutil.ReadFile(YIG_CONF_PATH)
	if err != nil {
		if err != nil {
			panic("Cannot open yig.toml")
		}
	}
	var c Config
	_, err = toml.Decode(string(data), &c)
	if err != nil {
		panic("load yig.toml error: " + err.Error())
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
	CONFIG.RedisPassword = c.RedisPassword
	CONFIG.DebugMode = c.DebugMode
	CONFIG.AdminKey = c.AdminKey
	CONFIG.LcDebug = c.LcDebug
	CONFIG.CephConfigPattern = c.CephConfigPattern
	CONFIG.ReservedOrigins = c.ReservedOrigins
	CONFIG.TidbInfo = c.TidbInfo
	CONFIG.KeepAlive = c.KeepAlive
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
	        c.HbaseTimeout).(time.Duration)
	CONFIG.GcThread = Ternary(c.GcThread == 0,
			1, c.GcThread).(int)
	CONFIG.LcThread = Ternary(c.LcThread == 0,
			1, c.LcThread).(int)
	CONFIG.LogLevel = Ternary(c.LogLevel == 0, 5, c.LogLevel).(int)
	CONFIG.MetaStore = Ternary(c.MetaStore == "", "tidb", c.MetaStore).(string)
	return nil
}