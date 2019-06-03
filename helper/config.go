package helper

import (
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	YIG_CONF_PATH = "/etc/yig/yig.toml"
	MIN_DOWNLOAD_BUFPOOL_SIZE = 512 << 10  // 512k
	MAX_DOWNLOAD_BUFPOOL_SIZE = 8 << 20    // 8M
)

type Config struct {
	S3Domain         []string                `toml:"s3domain"` // Domain name of YIG
	Region           string                  `toml:"region"`   // Region name this instance belongs to, e.g cn-bj-1
	Plugins          map[string]PluginConfig          `toml:"plugins"`
	LogPath          string                  `toml:"log_path"`
	AccessLogPath    string                  `toml:"access_log_path"`
	AccessLogFormat  string                  `toml:"access_log_format"`
	PanicLogPath     string                  `toml:"panic_log_path"`
	PidFile          string                  `toml:"pid_file"`
	BindApiAddress   string                  `toml:"api_listener"`
	BindAdminAddress string                  `toml:"admin_listener"`
	SSLKeyPath       string                  `toml:"ssl_key_path"`
	SSLCertPath      string                  `toml:"ssl_cert_path"`
	ZookeeperAddress string                  `toml:"zk_address"`

	InstanceId             string // if empty, generated one at server startup
	ConcurrentRequestLimit int
	HbaseZnodeParent       string        // won't change default("/hbase") if leave this option empty
	HbaseTimeout           time.Duration // in seconds
	DebugMode              bool          `toml:"debug_mode"`
	AdminKey               string        `toml:"admin_key"` //used for tools/admin to communicate with yig
	GcThread               int           `toml:"gc_thread"`
	LcThread               int           //used for tools/lc only, set worker numbers to do lc
	LcDebug                bool          //used for tools/lc only, if this was set true, will treat days as seconds
	LogLevel               int           `toml:"log_level"` //1-20
	CephConfigPattern      string        `toml:"ceph_config_pattern"`
	ReservedOrigins        string        `toml:"reserved_origins"` // www.ccc.com,www.bbb.com,127.0.0.1
	MetaStore              string        `toml:"meta_store"`
	TidbInfo               string        `toml:"tidb_info"`
	KeepAlive              bool          `toml:"keepalive"`

	//About cache
	RedisAddress            string   `toml:"redis_address"`           // redis connection string, e.g localhost:1234
	RedisConnectionNumber   int      `toml:"redis_connection_number"` // number of connections to redis(i.e max concurrent request number)
	RedisPassword           string   `toml:"redis_password"`          // redis auth password
	MetaCacheType           int      `toml:"meta_cache_type"`
	EnableDataCache         bool     `toml:"enable_data_cache"`
	RedisMode               int      `toml:"redis_mode"`
	RedisNodes              []string `toml:"redis_nodes"`
	RedisSentinelMasterName string   `toml:"redis_sentinel_master_name"`
	RedisConnectTimeout     int      `toml:"redis_connect_timeout"`
	RedisReadTimeout        int      `toml:"redis_read_timeout"`
	RedisWriteTimeout       int      `toml:"redis_write_timeout"`
	RedisKeepAlive          int      `toml:"redis_keepalive"`
	RedisPoolMaxIdle        int      `toml:"redis_pool_max_idle"`
	RedisPoolIdleTimeout    int      `toml:"redis_pool_idle_timeout"`

	// If the value is not 0, the cached ping detection will be turned on, and the interval is the number of seconds.
	CacheCircuitCheckInterval int `toml:"cache_circuit_check_interval"`
	// This property sets the amount of seconds, after tripping the circuit,
	// to reject requests before allowing attempts again to determine if the circuit should again be closed.
	CacheCircuitCloseSleepWindow int `toml:"cache_circuit_close_sleep_window"`
	// This value is how may consecutive passing requests are required before the circuit is closed
	CacheCircuitCloseRequiredCount int `toml:"cache_circuit_close_required_count"`
	// This property sets the minimum number of requests in a rolling window that will trip the circuit.
	CacheCircuitOpenThreshold int `toml:"cache_circuit_open_threshold"`

	DownLoadBufPoolSize int `toml:"download_buf_pool_size"`

	KMS KMSConfig `toml:"kms"`

	// Message Bus
	MsgBus MsgBusConfig `toml:"msg_bus"`
}

type PluginConfig struct {
        Path string  `toml:"path"`
        Enable bool  `toml:"enable"`
        Args  map[string]interface{} `toml:"args"`
}

type KMSConfig struct {
	Type     string
	Endpoint string
	Id       string `toml:"kms_id"`
	Secret   string `toml:"kms_secret"`
	Version  int
	Keyname  string
}

type MsgBusConfig struct {
	// Controls whether to enable message bus when receive the request.
	Enabled bool `toml:"msg_bus_enable"`
	// Controls the under implementation of message bus: 1 for kafka.
	Type int `toml:"msg_bus_type"`
	// Controls the message topic used by message bus.
	Topic string `toml:"msg_bus_topic"`
	// Controls the request timeout for sending a req through message bus.
	RequestTimeoutMs int `toml:"msg_bus_request_timeout_ms"`
	// Controls the total timeout for sending a req through message bus.
	// It will timeout if min(MessageTimeoutMs, SendMaxRetries * RequestTimeoutMs) meets.
	MessageTimeoutMs int `toml:"msg_bus_message_timeout_ms"`
	// Controls the retry time used by message bus if it fails to send a req.
	SendMaxRetries int `toml:"msg_bus_send_max_retries"`
	// Controls the settings for the implementation of message bus.
	// For kafka, the 'broker_list' must be set, like 'broker_list = "kafka:29092"'
	Server map[string]interface{} `toml:"msg_bus_server"`
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
	CONFIG.Plugins = c.Plugins
	CONFIG.LogPath = c.LogPath
	CONFIG.AccessLogPath = c.AccessLogPath
	CONFIG.AccessLogFormat = c.AccessLogFormat
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
	CONFIG.RedisMode = c.RedisMode
	CONFIG.RedisNodes = c.RedisNodes
	CONFIG.RedisSentinelMasterName = c.RedisSentinelMasterName
	CONFIG.RedisConnectionNumber = Ternary(c.RedisConnectionNumber == 0,
		10, c.RedisConnectionNumber).(int)
	CONFIG.EnableDataCache = c.EnableDataCache
	CONFIG.MetaCacheType = c.MetaCacheType
	CONFIG.RedisConnectTimeout = Ternary(c.RedisConnectTimeout < 0, 0, c.RedisConnectTimeout).(int)
	CONFIG.RedisReadTimeout = Ternary(c.RedisReadTimeout < 0, 0, c.RedisReadTimeout).(int)
	CONFIG.RedisWriteTimeout = Ternary(c.RedisWriteTimeout < 0, 0, c.RedisWriteTimeout).(int)
	CONFIG.RedisKeepAlive = Ternary(c.RedisKeepAlive < 0, 0, c.RedisKeepAlive).(int)
	CONFIG.RedisPoolMaxIdle = Ternary(c.RedisPoolMaxIdle < 0, 0, c.RedisPoolMaxIdle).(int)
	CONFIG.RedisPoolIdleTimeout = Ternary(c.RedisPoolIdleTimeout < 0, 0, c.RedisPoolIdleTimeout).(int)

	CONFIG.CacheCircuitCheckInterval = Ternary(c.CacheCircuitCheckInterval < 0, 0, c.CacheCircuitCheckInterval).(int)
	CONFIG.CacheCircuitCloseSleepWindow = Ternary(c.CacheCircuitCloseSleepWindow < 0, 0, c.CacheCircuitCloseSleepWindow).(int)
	CONFIG.CacheCircuitCloseRequiredCount = Ternary(c.CacheCircuitCloseRequiredCount < 0, 0, c.CacheCircuitCloseRequiredCount).(int)
	CONFIG.CacheCircuitOpenThreshold = Ternary(c.CacheCircuitOpenThreshold < 0, 0, c.CacheCircuitOpenThreshold).(int)

	CONFIG.DownLoadBufPoolSize = Ternary(c.DownLoadBufPoolSize < MIN_DOWNLOAD_BUFPOOL_SIZE || c.DownLoadBufPoolSize > MAX_DOWNLOAD_BUFPOOL_SIZE, MIN_DOWNLOAD_BUFPOOL_SIZE, c.DownLoadBufPoolSize).(int)

	CONFIG.KMS = c.KMS

	CONFIG.MsgBus = c.MsgBus
	CONFIG.MsgBus.RequestTimeoutMs = Ternary(c.MsgBus.RequestTimeoutMs == 0, 3000, c.MsgBus.RequestTimeoutMs).(int)
	CONFIG.MsgBus.MessageTimeoutMs = Ternary(c.MsgBus.MessageTimeoutMs == 0, 5000, c.MsgBus.MessageTimeoutMs).(int)
	CONFIG.MsgBus.SendMaxRetries = Ternary(c.MsgBus.SendMaxRetries == 0, 2, c.MsgBus.SendMaxRetries).(int)

	return nil
}
