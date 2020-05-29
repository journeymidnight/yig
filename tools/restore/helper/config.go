package helper

import "C"
import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

const (
	configPath      = "/etc/yig/yig-restore.toml"
	MIN_BUFFER_SIZE = 512 << 10 // 512k
	MAX_BUFEER_SIZE = 8 << 20   // 8M
	DEFAULTSPEC     = "@every 1h"
	DEFAULTLOCK     = 45 // 45min
	DEFAULTREFRESH  = 30 // 30 min
)

var Conf Config

type Config struct {
	InstanceId           string // if empty, generated one at server startup
	LogPath              string `toml:"log_path"`
	LogLevel             string `toml:"log_level"` // "info", "warn", "error"
	PiggybackUpdateUsage bool   `toml:"piggyback_update_usage"`
	EnableCompression    bool   `toml:"enable_compression"`

	EnableRestoreObjectCron bool   `toml:"enable_restore_object_cron"`
	RestoreObjectSpec       string `toml:"restore_object_spec"`
	LockTime                int    `toml:"lock_time"`
	RefreshLockTime         int    `toml:"refresh_lock_time"`

	CephConfigPattern   string `toml:"ceph_config_pattern"`
	DownloadBufPoolSize int64  `toml:"download_buf_pool_size"`
	UploadMinChunkSize  int64  `toml:"upload_min_chunk_size"`
	UploadMaxChunkSize  int64  `toml:"upload_max_chunk_size"`

	DBStore  string `toml:"db_store"`
	TidbInfo string `toml:"tidb_info"`
	// DB Connection parameters
	DbMaxOpenConns       int `toml:"db_max_open_conns"`
	DbMaxIdleConns       int `toml:"db_max_idle_conns"`
	DbConnMaxLifeSeconds int `toml:"db_conn_max_life_seconds"`

	RedisStore           string   `toml:"redis_store"`   // Choose redis connection method
	RedisAddress         string   `toml:"redis_address"` // redis connection string, e.g localhost:1234
	RedisGroup           []string `toml:"redis_group"`
	RedisPassword        string   `toml:"redis_password"` // redis auth password
	RedisMaxRetries      int      `toml:"redis_max_retries"`
	RedisConnectTimeout  int      `toml:"redis_connect_timeout"`
	RedisReadTimeout     int      `toml:"redis_read_timeout"`
	RedisWriteTimeout    int      `toml:"redis_write_timeout"`
	RedisPoolIdleTimeout int      `toml:"redis_pool_idle_timeout"`

	Plugins map[string]PluginConfig `toml:"plugins"`
}

type PluginConfig struct {
	Path   string                 `toml:"path"`
	Enable bool                   `toml:"enable"`
	Args   map[string]interface{} `toml:"args"`
}

func ReadConfig() {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		if err != nil {
			panic("[ERROR] Cannot open /etc/yig/yig-restore.toml")
		}
	}
	var c Config
	_, err = toml.Decode(string(data), &c)
	if err != nil {
		panic("[ERROR] Load yig-restore.toml error: " + err.Error())
	}
	Conf.InstanceId = Ternary(c.InstanceId == "",
		string(GenerateRandomId()), c.InstanceId).(string)
	Conf.LogPath = c.LogPath
	Conf.LogLevel = Ternary(len(c.LogLevel) == 0, "info", c.LogLevel).(string)
	Conf.PiggybackUpdateUsage = c.PiggybackUpdateUsage
	Conf.EnableCompression = c.EnableCompression
	Conf.EnableRestoreObjectCron = c.EnableRestoreObjectCron
	Conf.RestoreObjectSpec = Ternary(c.RestoreObjectSpec == "" && Conf.EnableRestoreObjectCron == true, DEFAULTSPEC, c.RestoreObjectSpec).(string)
	Conf.LockTime = Ternary(c.LockTime < 0, DEFAULTLOCK, c.LockTime).(int)
	Conf.RefreshLockTime = Ternary(c.RefreshLockTime < 0, DEFAULTREFRESH, c.RefreshLockTime).(int)
	Conf.CephConfigPattern = c.CephConfigPattern
	Conf.DownloadBufPoolSize = Ternary(c.DownloadBufPoolSize < MIN_BUFFER_SIZE || c.DownloadBufPoolSize > MAX_BUFEER_SIZE, MIN_BUFFER_SIZE, c.DownloadBufPoolSize).(int64)
	Conf.UploadMinChunkSize = Ternary(c.UploadMinChunkSize < MIN_BUFFER_SIZE || c.UploadMinChunkSize > MAX_BUFEER_SIZE, MIN_BUFFER_SIZE, c.UploadMinChunkSize).(int64)
	Conf.UploadMaxChunkSize = Ternary(c.UploadMaxChunkSize < Conf.UploadMinChunkSize || c.UploadMaxChunkSize > MAX_BUFEER_SIZE, MAX_BUFEER_SIZE, c.UploadMaxChunkSize).(int64)
	Conf.DBStore = c.DBStore
	Conf.TidbInfo = c.TidbInfo
	Conf.DbMaxOpenConns = Ternary(c.DbMaxOpenConns < 0, 0, c.DbMaxOpenConns).(int)
	Conf.DbMaxIdleConns = Ternary(c.DbMaxIdleConns < 0, 0, c.DbMaxIdleConns).(int)
	Conf.DbConnMaxLifeSeconds = Ternary(c.DbConnMaxLifeSeconds < 0, 0, c.DbConnMaxLifeSeconds).(int)
	Conf.RedisStore = Ternary(c.RedisStore == "", "single", c.RedisStore).(string)
	Conf.RedisAddress = c.RedisAddress
	Conf.RedisGroup = c.RedisGroup
	Conf.RedisPassword = c.RedisPassword
	Conf.RedisMaxRetries = Ternary(c.RedisMaxRetries < 0, 1000, c.RedisMaxRetries).(int)
	Conf.RedisConnectTimeout = Ternary(c.RedisConnectTimeout < 0, 0, c.RedisConnectTimeout).(int)
	Conf.RedisReadTimeout = Ternary(c.RedisReadTimeout < 0, 0, c.RedisReadTimeout).(int)
	Conf.RedisWriteTimeout = Ternary(c.RedisWriteTimeout < 0, 0, c.RedisWriteTimeout).(int)
	Conf.RedisPoolIdleTimeout = Ternary(c.RedisPoolIdleTimeout < 0, 0, c.RedisPoolIdleTimeout).(int)
	Conf.Plugins = c.Plugins
}
