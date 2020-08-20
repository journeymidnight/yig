package helper

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	YIG_CONF_PATH         = "/etc/yig/yig.toml"
	MIN_BUFFER_SIZE int64 = 512 << 10 // 512k
	MAX_BUFEER_SIZE int64 = 8 << 20   // 8M

	DEFAULTLOCK    = 45 // 45min
	DEFAULTREFRESH = 30 // 30 min

)

type Config struct {
	S3Domain             []string                `toml:"s3domain"` // Domain name of YIG
	Region               string                  `toml:"region"`   // Region name this instance belongs to, e.g cn-bj-1
	Plugins              map[string]PluginConfig `toml:"plugins"`
	PiggybackUpdateUsage bool                    `toml:"piggyback_update_usage"`
	LogPath              string                  `toml:"log_path"`
	AccessLogPath        string                  `toml:"access_log_path"`
	AccessLogFormat      string                  `toml:"access_log_format"`
	PanicLogPath         string                  `toml:"panic_log_path"`
	PidFile              string                  `toml:"pid_file"`
	BindApiAddress       string                  `toml:"api_listener"`
	BindAdminAddress     string                  `toml:"admin_listener"`
	SSLKeyPath           string                  `toml:"ssl_key_path"`
	SSLCertPath          string                  `toml:"ssl_cert_path"`

	InstanceId             string // if empty, generated one at server startup
	ConcurrentRequestLimit int

	MgThread         int `toml:"mg_thread"`
	MgScanInterval   int `toml:"mg_scan_interval"`
	MgObjectCooldown int `toml:"mg_object_cooldown"`

	DebugMode         bool     `toml:"debug_mode"`
	EnablePProf       bool     `toml:"enable_pprof"`
	BindPProfAddress  string   `toml:"pprof_listener"`
	AdminKey          string   `toml:"admin_key"` //used for tools/admin to communicate with yig
	LcThread          int      //used for tools/lc only, set worker numbers to do lc
	LogLevel          string   `toml:"log_level"` // "info", "warn", "error"
	CephConfigPattern string   `toml:"ceph_config_pattern"`
	ReservedOrigins   string   `toml:"reserved_origins"` // www.ccc.com,www.bbb.com,127.0.0.1
	MetaStore         string   `toml:"meta_store"`
	TidbInfo          string   `toml:"tidb_info"`
	PdAddress         []string `toml:"pd_address"`
	KeepAlive         bool     `toml:"keepalive"`
	EnableCompression bool     `toml:"enable_compression"`
	KafkaBrokers      []string `toml:"kafka_brokers"` // list of `IP:Port`
	GcTopic           string   `toml:"gc_topic"`

	LifecycleSpec string `toml:"lifecycle_spec"` // use for Lifecycle timing

	//About cache
	EnableUsagePush       bool     `toml:"enable_usage_push"`
	RedisStore            string   `toml:"redis_store"`   // Choose redis connection method
	RedisAddress          string   `toml:"redis_address"` // redis connection string, e.g localhost:1234
	RedisGroup            []string `toml:"redis_group"`
	RedisConnectionNumber int      `toml:"redis_connection_number"` // number of connections to redis(i.e max concurrent request number)
	RedisPassword         string   `toml:"redis_password"`          // redis auth password
	MetaCacheType         int      `toml:"meta_cache_type"`
	MetaCacheTTL          int      `toml:"meta_cache_ttl"` //ttl second
	EnableDataCache       bool     `toml:"enable_data_cache"`
	RedisMaxRetries       int      `toml:"redis_max_retries"`
	RedisConnectTimeout   int      `toml:"redis_connect_timeout"`
	RedisReadTimeout      int      `toml:"redis_read_timeout"`
	RedisWriteTimeout     int      `toml:"redis_write_timeout"`
	RedisKeepAlive        int      `toml:"redis_keepalive"`
	RedisPoolMaxIdle      int      `toml:"redis_pool_max_idle"`
	RedisPoolIdleTimeout  int      `toml:"redis_pool_idle_timeout"`
	RedisMinIdleConns     int      `toml:"redis_min_idle_conns"`

	// DB Connection parameters
	DbMaxOpenConns       int `toml:"db_max_open_conns"`
	DbMaxIdleConns       int `toml:"db_max_idle_conns"`
	DbConnMaxLifeSeconds int `toml:"db_conn_max_life_seconds"`

	// If the value is not 0, the cached ping detection will be turned on, and the interval is the number of seconds.
	CacheCircuitCheckInterval int `toml:"cache_circuit_check_interval"`
	// This property sets the amount of seconds, after tripping the circuit,
	// to reject requests before allowing attempts again to determine if the circuit should again be closed.
	CacheCircuitCloseSleepWindow int `toml:"cache_circuit_close_sleep_window"`
	// This value is how may consecutive passing requests are required before the circuit is closed
	CacheCircuitCloseRequiredCount int `toml:"cache_circuit_close_required_count"`
	// This property sets the minimum number of requests in a rolling window that will trip the circuit.
	CacheCircuitOpenThreshold     int   `toml:"cache_circuit_open_threshold"`
	CacheCircuitExecTimeout       uint  `toml:"cache_circuit_exec_timeout"`
	CacheCircuitExecMaxConcurrent int64 `toml:"cache_circuit_exec_max_concurrent"`

	StsEncryptionKey string `toml:"sts_encryption_key"`

	DownloadBufPoolSize int64 `toml:"download_buf_pool_size"`
	UploadMinChunkSize  int64 `toml:"upload_min_chunk_size"`
	UploadMaxChunkSize  int64 `toml:"upload_max_chunk_size"`
	BigFileThreshold    int64 `toml:"big_file_threshold"`

	EnableQoS            bool `toml:"enable_qos"`
	DefaultReadOps       int  `toml:"default_read_ops"`
	DefaultWriteOps      int  `toml:"default_write_ops"`
	DefaultBandwidthKBps int  `toml:"default_bandwidth_kbps"`

	// The switch that decides whether to use the real thawing logic,
	// if it is on, the thawing logic is a false thawing mode that only modifies the database state
	FakeRestore      bool     `toml:"fake_restore"`
	LockTime         int      `toml:"lock_time"`
	RefreshLockTime  int      `toml:"refresh_lock_time"`
	LogDeliveryGroup []string `toml:"log_delivery_group"`
}

type PluginConfig struct {
	Path   string                 `toml:"path"`
	Enable bool                   `toml:"enable"`
	Args   map[string]interface{} `toml:"args"`
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
	CONFIG.PiggybackUpdateUsage = c.PiggybackUpdateUsage
	CONFIG.LogPath = c.LogPath
	CONFIG.AccessLogPath = c.AccessLogPath
	CONFIG.AccessLogFormat = c.AccessLogFormat
	CONFIG.PanicLogPath = c.PanicLogPath
	CONFIG.PidFile = c.PidFile
	CONFIG.BindApiAddress = c.BindApiAddress
	CONFIG.BindAdminAddress = c.BindAdminAddress
	CONFIG.SSLKeyPath = c.SSLKeyPath
	CONFIG.SSLCertPath = c.SSLCertPath
	CONFIG.DebugMode = c.DebugMode
	CONFIG.EnablePProf = c.EnablePProf
	CONFIG.BindPProfAddress = c.BindPProfAddress
	CONFIG.AdminKey = c.AdminKey
	CONFIG.CephConfigPattern = c.CephConfigPattern
	CONFIG.ReservedOrigins = c.ReservedOrigins
	CONFIG.TidbInfo = c.TidbInfo
	CONFIG.PdAddress = c.PdAddress
	CONFIG.KeepAlive = c.KeepAlive
	CONFIG.EnableCompression = c.EnableCompression
	CONFIG.KafkaBrokers = c.KafkaBrokers
	CONFIG.GcTopic = c.GcTopic
	CONFIG.InstanceId = Ternary(c.InstanceId == "",
		string(GenerateRandomId()), c.InstanceId).(string)
	CONFIG.ConcurrentRequestLimit = Ternary(c.ConcurrentRequestLimit == 0,
		10000, c.ConcurrentRequestLimit).(int)
	CONFIG.LcThread = Ternary(c.LcThread == 0,
		1, c.LcThread).(int)
	CONFIG.MgThread = Ternary(c.MgThread == 0,
		1, c.MgThread).(int)
	CONFIG.MgScanInterval = Ternary(c.MgScanInterval == 0,
		600, c.MgScanInterval).(int)
	CONFIG.MgObjectCooldown = Ternary(c.MgObjectCooldown == 0,
		3600, c.MgObjectCooldown).(int)
	CONFIG.LogLevel = Ternary(len(c.LogLevel) == 0, "info", c.LogLevel).(string)
	CONFIG.StsEncryptionKey = Ternary(len(c.StsEncryptionKey) == 0, "hehehehehehehehehehehehehehehehe", c.StsEncryptionKey).(string)
	CONFIG.MetaStore = Ternary(c.MetaStore == "", "tidb", c.MetaStore).(string)

	CONFIG.LifecycleSpec = Ternary(c.LifecycleSpec == "", "@midnight", c.LifecycleSpec).(string)

	CONFIG.EnableUsagePush = c.EnableUsagePush
	CONFIG.RedisStore = Ternary(c.RedisStore == "", "single", c.RedisStore).(string)
	CONFIG.RedisAddress = c.RedisAddress
	CONFIG.EnableUsagePush = c.EnableUsagePush
	CONFIG.RedisGroup = c.RedisGroup
	CONFIG.RedisPassword = c.RedisPassword
	CONFIG.RedisConnectionNumber = Ternary(c.RedisConnectionNumber == 0,
		10, c.RedisConnectionNumber).(int)
	CONFIG.MetaCacheTTL = Ternary(c.MetaCacheTTL < 30, 30, c.MetaCacheTTL).(int)
	CONFIG.EnableDataCache = c.EnableDataCache
	CONFIG.RedisMaxRetries = Ternary(c.RedisMaxRetries < 0, 1000, c.RedisMaxRetries).(int)
	CONFIG.MetaCacheType = c.MetaCacheType
	CONFIG.RedisConnectTimeout = Ternary(c.RedisConnectTimeout < 0, 0, c.RedisConnectTimeout).(int)
	CONFIG.RedisReadTimeout = Ternary(c.RedisReadTimeout < 0, 0, c.RedisReadTimeout).(int)
	CONFIG.RedisWriteTimeout = Ternary(c.RedisWriteTimeout < 0, 0, c.RedisWriteTimeout).(int)
	CONFIG.RedisKeepAlive = Ternary(c.RedisKeepAlive < 0, 0, c.RedisKeepAlive).(int)
	CONFIG.RedisPoolMaxIdle = Ternary(c.RedisPoolMaxIdle < 0, 0, c.RedisPoolMaxIdle).(int)
	CONFIG.RedisPoolIdleTimeout = Ternary(c.RedisPoolIdleTimeout < 0, 0, c.RedisPoolIdleTimeout).(int)
	CONFIG.RedisMinIdleConns = Ternary(c.RedisMinIdleConns < 0, 12, c.RedisMinIdleConns).(int)

	CONFIG.DbMaxOpenConns = Ternary(c.DbMaxOpenConns < 0, 0, c.DbMaxOpenConns).(int)
	CONFIG.DbMaxIdleConns = Ternary(c.DbMaxIdleConns < 0, 0, c.DbMaxIdleConns).(int)
	CONFIG.DbConnMaxLifeSeconds = Ternary(c.DbConnMaxLifeSeconds < 0, 0, c.DbConnMaxLifeSeconds).(int)

	CONFIG.CacheCircuitCheckInterval = Ternary(c.CacheCircuitCheckInterval < 0, 0, c.CacheCircuitCheckInterval).(int)
	CONFIG.CacheCircuitCloseSleepWindow = Ternary(c.CacheCircuitCloseSleepWindow < 0, 0, c.CacheCircuitCloseSleepWindow).(int)
	CONFIG.CacheCircuitCloseRequiredCount = Ternary(c.CacheCircuitCloseRequiredCount < 0, 0, c.CacheCircuitCloseRequiredCount).(int)
	CONFIG.CacheCircuitOpenThreshold = Ternary(c.CacheCircuitOpenThreshold < 0, 0, c.CacheCircuitOpenThreshold).(int)
	CONFIG.CacheCircuitExecTimeout = Ternary(c.CacheCircuitExecTimeout == 0, 1, c.CacheCircuitExecTimeout).(uint)
	CONFIG.CacheCircuitExecMaxConcurrent = c.CacheCircuitExecMaxConcurrent

	CONFIG.DownloadBufPoolSize = Ternary(c.DownloadBufPoolSize < MIN_BUFFER_SIZE || c.DownloadBufPoolSize > MAX_BUFEER_SIZE, MIN_BUFFER_SIZE, c.DownloadBufPoolSize).(int64)
	CONFIG.UploadMinChunkSize = Ternary(c.UploadMinChunkSize < MIN_BUFFER_SIZE || c.UploadMinChunkSize > MAX_BUFEER_SIZE, MIN_BUFFER_SIZE, c.UploadMinChunkSize).(int64)
	CONFIG.UploadMaxChunkSize = Ternary(c.UploadMaxChunkSize < CONFIG.UploadMinChunkSize || c.UploadMaxChunkSize > MAX_BUFEER_SIZE, MAX_BUFEER_SIZE, c.UploadMaxChunkSize).(int64)
	CONFIG.BigFileThreshold = Ternary(c.BigFileThreshold == 0, int64(1048576), c.BigFileThreshold).(int64)

	CONFIG.EnableQoS = c.EnableQoS
	CONFIG.DefaultReadOps = Ternary(c.DefaultReadOps <= 0, 2000, c.DefaultReadOps).(int)
	CONFIG.DefaultWriteOps = Ternary(c.DefaultWriteOps <= 0, 1000, c.DefaultWriteOps).(int)
	CONFIG.DefaultBandwidthKBps = Ternary(c.DefaultBandwidthKBps <= 0, 102400, c.DefaultBandwidthKBps).(int)

	CONFIG.FakeRestore = c.FakeRestore
	CONFIG.LockTime = Ternary(c.LockTime <= 0, DEFAULTLOCK, c.LockTime).(int)
	CONFIG.RefreshLockTime = Ternary(c.RefreshLockTime <= 0, DEFAULTREFRESH, c.RefreshLockTime).(int)
	CONFIG.LogDeliveryGroup = c.LogDeliveryGroup
	return nil
}

// Convert from "/var/log/yig/yig.log" to something like "/var/log/yig/49106.yig.log"
// Only support UNIX style path
func logFilePathWithPid(rawPath string) string {
	pid := os.Getpid()
	parts := strings.Split(rawPath, "/")
	parts[len(parts)-1] = fmt.Sprintf("%d.%s", pid, parts[len(parts)-1])
	return strings.Join(parts, "/")
}
