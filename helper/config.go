package helper

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"time"
)

const (
	YIG_CONF_PATH = "/etc/yig/yig.toml"
)

type Config struct {
	S3Domain                   string `toml:"s3domain"`     // Domain name of YIG
	Region                     string `toml:"region"`       // Region name this instance belongs to, e.g cn-bj-1
	IamEndpoint                string `toml:"iam_endpoint"` // le IAM endpoint address
	IamKey                     string `toml:"iam_key"`
	IamSecret                  string `toml:"iam_secret"`
	IamVersion                 string `toml:"iam_version"`
	LogPath                    string `toml:"log_path"`
	PanicLogPath               string `toml:"panic_log_path"`
	PidFile                    string `toml:"pid_file"`
	BindApiAddress             string `toml:"api_listener"`
	BindAdminAddress           string `toml:"admin_listener"`
	SSLKeyPath                 string `toml:"ssl_key_path"`
	SSLCertPath                string `toml:"ssl_cert_path"`
	ZookeeperAddress           string `toml:"zk_address"`

	InstanceId                 string // if empty, generated one at server startup
	ConcurrentRequestLimit     int
	HbaseZnodeParent           string        // won't change default("/hbase") if leave this option empty
	HbaseTimeout               time.Duration // in seconds
	DebugMode                  bool          `toml:"debug_mode"`
	AdminKey                   string        `toml:"admin_key"` //used for tools/admin to communicate with yig
	GcThread                   int    `toml:"gc_thread"`
	LcThread                   int    //used for tools/lc only, set worker numbers to do lc
	LcDebug                    bool   //used for tools/lc only, if this was set true, will treat days as seconds
	LogLevel                   int    `toml:"log_level"` //1-20
	CephConfigPattern          string `toml:"ceph_config_pattern"`
	ReservedOrigins            string `toml:"reserved_origins"`// www.ccc.com,www.bbb.com,127.0.0.1
	MetaStore                  string `toml:"meta_store"`
	TidbInfo                   string `toml:"tidb_info"`
	KeepAlive                  bool   `toml:"keepalive"`

	//About cache
	RedisAddress                   string `toml:"redis_address"`           // redis connection string, e.g localhost:1234
	RedisConnectionNumber          int    `toml:"redis_connection_number"` // number of connections to redis(i.e max concurrent request number)
	RedisPassword                  string `toml:"redis_password"`	                             // redis auth password
	MetaCacheType                  int    `toml:"meta_cache_type"`
	EnableDataCache                bool   `toml:"enable_data_cache"`
	RedisConnectTimeout            int    `toml:"redis_connect_timeout"`
	RedisReadTimeout               int    `toml:"redis_read_timeout"`
	RedisWriteTimeout              int    `toml:"redis_write_timeout"`
	RedisKeepAlive                 int    `toml:"redis_keepalive"`
	RedisPoolMaxIdle               int    `toml:"redis_pool_max_idle"`
	RedisPoolIdleTimeout           int    `toml:"redis_pool_idle_timeout"`

	// If the value is not 0, the cached ping detection will be turned on, and the interval is the number of seconds.
	CacheCircuitCheckInterval      int    `toml:"cache_circuit_check_interval"`
	// This property sets the amount of seconds, after tripping the circuit,
	// to reject requests before allowing attempts again to determine if the circuit should again be closed.
	CacheCircuitCloseSleepWindow   int    `toml:"cache_circuit_close_sleep_window"`
	// This value is how may consecutive passing requests are required before the circuit is closed
	CacheCircuitCloseRequiredCount int    `toml:"cache_circuit_close_required_count"`
	// This property sets the minimum number of requests in a rolling window that will trip the circuit.
	CacheCircuitOpenThreshold      int    `toml:"cache_circuit_open_threshold"`

	KMS KMSConfig `toml:"kms"`
}

type KMSConfig struct {
	Type     string
	Endpoint string
	Id       string `toml:"kms_id"`
	Secret   string `toml:"kms_secret"`
	Version  int
	Keyname  string
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
	CONFIG.IamVersion = Ternary(c.IamVersion == "",
		"v1", c.IamVersion).(string)
	CONFIG.LogPath = c.LogPath
	CONFIG.PanicLogPath = c.PanicLogPath
	CONFIG.PidFile = c.PidFile
	CONFIG.BindApiAddress = c.BindApiAddress
	CONFIG.BindAdminAddress = c.BindAdminAddress
	CONFIG.SSLKeyPath = c.SSLKeyPath
	CONFIG.SSLCertPath = c.SSLCertPath
	CONFIG.ZookeeperAddress = c.ZookeeperAddress
	CONFIG.DebugMode = c.DebugMode
	CONFIG.AdminKey = c.AdminKey
	CONFIG.LcDebug = c.LcDebug
	CONFIG.CephConfigPattern = c.CephConfigPattern
	CONFIG.ReservedOrigins = c.ReservedOrigins
	CONFIG.TidbInfo = c.TidbInfo
	CONFIG.KeepAlive = c.KeepAlive
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

	CONFIG.RedisAddress = c.RedisAddress
	CONFIG.RedisPassword = c.RedisPassword
	CONFIG.RedisConnectionNumber = Ternary(c.RedisConnectionNumber == 0,
		10, c.RedisConnectionNumber).(int)
	CONFIG.EnableDataCache = c.EnableDataCache
	CONFIG.MetaCacheType = c.MetaCacheType
	CONFIG.RedisConnectTimeout = Ternary(c.RedisConnectTimeout < 0, 0, c.RedisConnectTimeout).(int)
	CONFIG.RedisReadTimeout = Ternary(c.RedisReadTimeout < 0, 0, c.RedisReadTimeout).(int)
	CONFIG.RedisWriteTimeout = Ternary(c.RedisWriteTimeout < 0, 0, c.RedisWriteTimeout).(int)
	CONFIG.RedisKeepAlive = Ternary(c.RedisKeepAlive < 0, 0, c.RedisKeepAlive).(int)
	CONFIG.RedisPoolMaxIdle = Ternary(c.RedisPoolMaxIdle < 0, 0, c.RedisPoolMaxIdle).(int)
	CONFIG.RedisPoolIdleTimeout = Ternary(c.RedisPoolIdleTimeout < 0 , 0, c.RedisPoolIdleTimeout).(int)

	CONFIG.CacheCircuitCheckInterval = Ternary(c.CacheCircuitCheckInterval < 0, 0, c.CacheCircuitCheckInterval).(int)
	CONFIG.CacheCircuitCloseSleepWindow = Ternary(c.CacheCircuitCloseSleepWindow < 0, 0, c.CacheCircuitCloseSleepWindow).(int)
	CONFIG.CacheCircuitCloseRequiredCount = Ternary(c.CacheCircuitCloseRequiredCount < 0, 0, c.CacheCircuitCloseRequiredCount).(int)
	CONFIG.CacheCircuitOpenThreshold = Ternary(c.CacheCircuitOpenThreshold < 0, 0, c.CacheCircuitOpenThreshold).(int)

	CONFIG.KMS = c.KMS

	return nil
}
